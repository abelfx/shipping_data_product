import os
import json
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATA_PATH = Path("data/raw/telegram_messages")

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
)
cursor = conn.cursor()

insert_sql = """
INSERT INTO raw.telegram_messages (
    message_id,
    channel_name,
    message_date,
    message_text,
    views,
    forwards,
    has_media,
    image_path
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

for date_dir in DATA_PATH.iterdir():
    if date_dir.is_dir():
        for json_file in date_dir.glob("*.json"):
            with open(json_file, "r", encoding="utf-8") as f:
                messages = json.load(f)

            for msg in messages:
                cursor.execute(
                    insert_sql,
                    (
                        msg["message_id"],
                        msg["channel_name"],
                        msg["message_date"],
                        msg["message_text"],
                        msg["views"],
                        msg["forwards"],
                        msg["has_media"],
                        msg["image_path"],
                    ),
                )

conn.commit()
cursor.close()
conn.close()
