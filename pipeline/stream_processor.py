import os
import json
import re
import logging
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RAW_NEWS_TOPIC = "raw-news"
PROCESSED_NEWS_TOPIC = "processed-news"

def clean_text(text: str) -> str:
    """Remove HTML tags, extra whitespace, and normalize the text."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

async def process_and_forward():
    consumer = AIOKafkaConsumer(
        RAW_NEWS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="processor-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    await consumer.start()
    await producer.start()
    logger.info("Stream Processor is running...")

    try:
        async for msg in consumer:
            raw = msg.value
            cleaned_title = clean_text(raw.get("title", ""))

            if not cleaned_title:
                logger.warning(f"Skipping empty article: {raw.get('news_id')}")
                continue

            # Build the processed payload
            processed = {
                "news_id": raw["news_id"],
                "company_ticker": raw["company_ticker"],
                "source_name": raw["source_name"],
                "title": cleaned_title,
                "link": raw.get("link", ""),
                "published_at": raw.get("published_at", ""),
                "ingested_at": raw.get("ingested_at", ""),
            }

            await producer.send_and_wait(PROCESSED_NEWS_TOPIC, processed)
            logger.info(f"Forwarded [{raw['company_ticker']}] → processed-news: {cleaned_title[:80]}")

    finally:
        await consumer.stop()
        await producer.stop()

if __name__ == "__main__":
    try:
        asyncio.run(process_and_forward())
    except KeyboardInterrupt:
        logger.info("Stream Processor shutting down.")
