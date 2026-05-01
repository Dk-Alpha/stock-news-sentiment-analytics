import asyncio
import aiohttp
import feedparser
import yaml
import json
import hashlib
import os
import logging
from pathlib import Path
from datetime import datetime, timezone
from aiokafka import AIOKafkaProducer
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)

# Resolve paths relative to this file's directory
BASE_DIR = Path(__file__).parent
load_dotenv(dotenv_path=BASE_DIR / ".env")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", 60))
RAW_NEWS_TOPIC = "raw-news"


def load_sources():
    sources_path = BASE_DIR / "sources.yaml"
    try:
        with open(sources_path, "r") as f:
            config = yaml.safe_load(f)
            return config.get("sources", [])
    except Exception as e:
        logger.error(f"Failed to load {sources_path}: {e}")
        return []


def generate_hash(title, link, published):
    hasher = hashlib.sha256()
    content = f"{title}-{link}-{published}".encode("utf-8")
    hasher.update(content)
    return hasher.hexdigest()


async def fetch_rss(session, url, fetch_type="rss"):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) NewsIntelligenceBot/1.0"}
    try:
        async with session.get(url, headers=headers, timeout=15) as response:
            if response.status == 200:
                html = await response.text()
                return feedparser.parse(html)
            else:
                logger.warning(f"Error {response.status} fetching {url}")
                return None
    except Exception as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None


async def process_source(source, session, producer, seen_hashes):
    source_type = source.get("type", "rss")
    logger.info(f"Polling {source_type} source: {source['name']}")
    feed = await fetch_rss(session, source["url"], source_type)
    if not feed or not feed.entries:
        return

    new_items_count = 0
    for entry in feed.entries:
        title = entry.get("title", "")
        link = entry.get("link", "")
        # Google News RSS usually publishes in 'published' or 'pubDate'
        published = entry.get("published", datetime.now(timezone.utc).isoformat())

        item_hash = generate_hash(title, link, published)

        if item_hash in seen_hashes:
            continue

        seen_hashes.add(item_hash)
        new_items_count += 1

        news_event = {
            "news_id": item_hash,
            "source_name": source["name"],
            "company_ticker": source.get("company_ticker", "UNKNOWN"),
            "title": title,
            "link": link,
            "published_at": published,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

        # Send to Kafka
        try:
            await producer.send_and_wait(
                RAW_NEWS_TOPIC, json.dumps(news_event).encode("utf-8")
            )
        except Exception as e:
            logger.error(f"Failed to send to Kafka: {e}")

    if new_items_count > 0:
        logger.info(f"Published {new_items_count} new articles from {source['name']}")


async def main():
    logger.info(f"Starting Ingester service. Connecting to Kafka: {KAFKA_BOOTSTRAP}")

    # Init Kafka Producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()
    logger.info("Kafka Producer started successfully.")

    seen_hashes = set()
    sources = load_sources()

    if not sources:
        logger.error("No sources configured. Exiting.")
        return

    try:
        async with aiohttp.ClientSession() as session:
            while True:
                # Reload sources dynamically on each cycle
                # This allows changing sources.yaml without restarting
                current_sources = load_sources()

                tasks = [
                    process_source(source, session, producer, seen_hashes)
                    for source in current_sources
                ]
                await asyncio.gather(*tasks)

                logger.info(f"Sleeping for {POLL_INTERVAL} seconds...")
                await asyncio.sleep(POLL_INTERVAL)
    finally:
        await producer.stop()
        logger.info("Kafka Producer stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Ingester service shutting down natively.")
