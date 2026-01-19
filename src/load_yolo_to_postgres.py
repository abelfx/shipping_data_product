import os
import csv
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

load_dotenv()

YOLO_OUTPUT_ROOT = Path("data/raw/yolo_detections")

DDL_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS raw;
"""

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS raw.image_detections (
    message_id BIGINT,
    channel_name TEXT,
    image_path TEXT,
    detected_class TEXT,
    confidence NUMERIC,
    image_category TEXT,
    detection_date DATE DEFAULT CURRENT_DATE
);
"""

INSERT_SQL = """
INSERT INTO raw.image_detections (
    message_id,
    channel_name,
    image_path,
    detected_class,
    confidence,
    image_category,
    detection_date
) VALUES (%s, %s, %s, %s, %s, %s, %s);
"""


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def find_csv_files(root: Path):
    for day_dir in sorted(root.iterdir()):
        if day_dir.is_dir():
            csv_path = day_dir / "detections.csv"
            if csv_path.exists():
                yield csv_path


def load_file(cursor, csv_path: Path):
    detection_date = csv_path.parent.name  # YYYY-MM-DD
    with csv_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            cursor.execute(
                INSERT_SQL,
                (
                    int(row["message_id"]) if row["message_id"] else None,
                    row["channel_name"],
                    row["image_path"],
                    row["detected_class"],
                    float(row["confidence"]) if row["confidence"] else None,
                    row["image_category"],
                    detection_date,
                ),
            )


def main():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(DDL_SCHEMA)
    cur.execute(DDL_TABLE)
    conn.commit()

    for csv_file in find_csv_files(YOLO_OUTPUT_ROOT):
        load_file(cur, csv_file)
        conn.commit()
        print(f"Loaded {csv_file}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
