import asyncio
from itertools import groupby
import os
from pathlib import Path
import shutil
import sqlite3

import aioboto3
from botocore.client import Config
from botocore.exceptions import ClientError

import psycopg2
from psycopg2.extensions import new_type, register_type

from transliterate import translit
import yaml

PG_DSN = "host=localhost dbname=thousands user=postgres password=postgres"
DEST_DIR = Path(__file__).parent.parent / "data"

S3_ENDPOINT = "https://s3.timeweb.cloud"
S3_BUCKET = "302f9aa7-62c4d4d3-ccfd-4077-86c8-cca52e0da376"

IMAGES_SRC_DIR = "/home/rush/thousands-backup/images"


def cast_point(value, cur):
    if value is None:
        return None
    try:
        # keep only 4 decimal places
        x, y = [round(c, 4) for c in map(float, value.strip("()").split(","))]
    except ValueError:
        raise psycopg2.InterfaceError("bad point representation: %r" % value)
    return (x, y)


async def upload_images_bulk(session, images):
    sem = asyncio.Semaphore(10)
    async with session.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        config=Config(s3={"addressing_style": "path"}),
    ) as client:

        async def sem_upload(client, path, key):
            async with sem:
                await upload_image(client, path, key)

        await asyncio.gather(*[sem_upload(client, path, key) for path, key in images])


async def upload_image(client, image_path, image_name):
    try:
        # Try to get the metadata of the object
        await client.head_object(Bucket=S3_BUCKET, Key=image_name)
        print(f"Image exists: {image_name}")
    except ClientError as e:
        # If the object does not exist, a 404 error will be raised
        if e.response["Error"]["Code"] == "404":
            await client.upload_file(
                image_path,
                S3_BUCKET,
                image_name,
                ExtraArgs={"ContentType": "image/jpeg"},
            )
            print(f"Uploaded {image_path} as {image_name}")
        else:
            raise


def gen_image_key(summit_id, comment):
    transliterated_comment = (
        translit(comment, "ru", reversed=True).replace(" ", "_").lower()
    )
    return f"{summit_id}_{transliterated_comment}"


def import_ridges(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, name, color from ridges")
        for row in cur:
            ridge_dir = DEST_DIR / row[0]
            os.mkdir(ridge_dir)
            meta = {"name": row[1], "color": row[2]}
            with open(ridge_dir / "_meta.yaml", "w") as f:
                yaml.dump(meta, f, allow_unicode=True)


def import_summits(conn, images_src_dir, s3_session):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, ridge_id, name, name_alt, height, 
                    description, interpretation, 
                    coordinates, image, preview, comment 
            FROM summits s
            LEFT JOIN summits_images si ON s.id = si.summit_id
            ORDER BY s.id, main, si.image DESC
        """
        )
        images_to_upload = []
        for summit, img_group in groupby(cur, key=lambda row: tuple(row[:8])):
            (
                summit_id,
                ridge_id,
                name,
                name_alt,
                height,
                description,
                interpretation,
                coordinates,
            ) = summit
            summit_data = {
                "name": name,
                "name_alt": name_alt,
                "height": height,
                "description": description,
                "interpretation": interpretation,
                "coordinates": list(coordinates),
                "images": [],
            }
            i = 0
            for img in img_group:
                image_filename, preview_filename, comment = img[8:]
                if image_filename is None:
                    continue
                image_path = Path(images_src_dir) / image_filename
                preview_path = Path(images_src_dir) / preview_filename

                image_key = gen_image_key(summit_id, comment)
                image_s3_key = f"summits/{image_key}_{i}.jpg"
                preview_s3_key = f"summits/{image_key}_{i}_preview.jpg"

                images_to_upload.append((image_path, image_s3_key))
                images_to_upload.append((preview_path, preview_s3_key))

                summit_data["images"].append(
                    {
                        "url": image_s3_key,
                        "preview_url": preview_s3_key,
                        "comment": comment,
                    }
                )
                i += 1
            summit_file = DEST_DIR / ridge_id / f"{summit_id}.yaml"
            with summit_file.open("w") as f:
                yaml.dump(summit_data, f, allow_unicode=True)
        asyncio.run(upload_images_bulk(s3_session, images_to_upload))


def import_users(conn, images_src_dir, s3_session, sqlite_conn):
    """
    CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                oauth_id TEXT NOT NULL,
                src INTEGER NOT NULL,
                name TEXT NOT NULL,
            )
    """
    images_to_upload = []
    sqlite_cur = sqlite_conn.cursor()
    with conn.cursor() as cur:
        cur.execute("SELECT id, oauth_id, src, name, image, preview FROM users")
        for row in cur:
            user_id, oauth_id, src, name, image, preview = row
            sqlite_cur.execute(
                "INSERT INTO users (id, oauth_id, src, name) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (user_id, oauth_id, src, name),
            )
            if image:
                for img, size in [(image, "M"), (preview, "S")]:
                    img_path = Path(images_src_dir) / img
                    if not img_path.exists():
                        continue
                    img_key = f"users/{user_id}_{size}.jpg"
                    images_to_upload.append((img_path, img_key))
                    sqlite_cur.execute(
                        "INSERT INTO user_images (user_id, size, url) VALUES (?, ?, ?)",
                        (user_id, size, img_key),
                    )
    asyncio.run(upload_images_bulk(s3_session, images_to_upload))
    sqlite_conn.commit()


def import_climbs(conn, sqlite_conn):
    sqlite_cur = sqlite_conn.cursor()
    with conn.cursor() as cur:
        cur.execute("SELECT user_id, summit_id, comment, year, month, day FROM climbs")
        for row in cur:
            user_id, summit_id, comment, year, month, day = row
            sqlite_cur.execute(
                "INSERT INTO climbs (user_id, summit_id, comment, year, month, day) VALUES (?, ?, ?, ?, ?, ?)",
                (user_id, summit_id, comment, year, month, day),
            )
    sqlite_conn.commit()


def main():
    shutil.rmtree(DEST_DIR, ignore_errors=True)
    os.mkdir(DEST_DIR)
    POINT = new_type((600,), "POINT", cast_point)
    register_type(POINT)

    conn = psycopg2.connect(PG_DSN)
    s3_session = aioboto3.Session()
    # s3_client = boto3.client(
    #    "s3",
    #    endpoint_url=S3_ENDPOINT,
    #    aws_access_key_id=os.environ["S3_ACCESS_KEY"],
    #    aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    #    config=Config(s3={"addressing_style": "path"}),
    # )
    sqlite_conn = sqlite3.connect("/home/rush/src/thousands2/thousands.sqlite")

    import_ridges(conn)
    import_summits(conn, IMAGES_SRC_DIR, s3_session)
    import_users(conn, IMAGES_SRC_DIR, s3_session, sqlite_conn)
    import_climbs(conn, sqlite_conn)


if __name__ == "__main__":
    main()
