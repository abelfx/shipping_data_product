import os
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto
from loguru import logger

# -------------------- Setup --------------------
load_dotenv()

API_ID = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")

BASE_DATA_PATH = Path("data/raw")
IMAGE_PATH = BASE_DATA_PATH / "images"
MESSAGE_PATH = BASE_DATA_PATH / "telegram_messages"

LOG_PATH = Path("logs")
LOG_PATH.mkdir(exist_ok=True)

logger.add(LOG_PATH / "scraper.log", rotation="1 MB")

CHANNELS = {
    "chemed": "https://t.me/CheMed123",
    "lobelia4cosmetics": "https://t.me/lobelia4cosmetics",
    "tikvahpharma": "https://t.me/tikvahpharma"
}

# -------------------- Scraper --------------------
async def scrape_channel(client, channel_name, channel_url):
    today = datetime.utcnow().strftime("%Y-%m-%d")
    output_dir = MESSAGE_PATH / today
    output_dir.mkdir(parents=True, exist_ok=True)

    image_dir = IMAGE_PATH / channel_name
    image_dir.mkdir(parents=True, exist_ok=True)

    messages_data = []

    logger.info(f"Scraping channel: {channel_name}")

    async for message in client.iter_messages(channel_url, limit=500):
        msg = {
            "message_id": message.id,
            "channel_name": channel_name,
            "message_date": message.date.isoformat() if message.date else None,
            "message_text": message.text,
            "views": message.views,
            "forwards": message.forwards,
            "has_media": bool(message.media),
            "image_path": None
        }

        # Download image if exists
        if isinstance(message.media, MessageMediaPhoto):
            image_file = image_dir / f"{message.id}.jpg"
            await client.download_media(message.photo, image_file)
            msg["image_path"] = str(image_file)

        messages_data.append(msg)

    # Save raw JSON
    output_file = output_dir / f"{channel_name}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(messages_data, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved {len(messages_data)} messages for {channel_name}")

# -------------------- Main --------------------
async def main():
    async with TelegramClient("session", API_ID, API_HASH) as client:
        for name, url in CHANNELS.items():
            try:
                await scrape_channel(client, name, url)
            except Exception as e:
                logger.error(f"Failed scraping {name}: {e}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
