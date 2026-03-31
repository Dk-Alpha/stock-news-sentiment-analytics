import os
import json
import logging
import asyncio
import psycopg2
from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_DB = os.getenv("POSTGRES_DB", "news_pipeline")
POSTGRES_USER = os.getenv("POSTGRES_USER", "pipeline_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "pipeline_pass")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

RAW_NEWS_TOPIC = "raw-news"
SENTIMENT_TOPIC = "sentiment-results"

def get_db_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT
    )

def handle_raw_news(msg_value, conn):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO live_news (news_id, source_name, company_ticker, title, link, published_at, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (news_id) DO NOTHING;
        """, (
            msg_value["news_id"],
            msg_value["source_name"],
            msg_value["company_ticker"],
            msg_value["title"],
            msg_value["link"],
            msg_value["published_at"],
            msg_value["ingested_at"]
        ))
        conn.commit()
    except Exception as e:
        logger.error(f"Error inserting raw news: {e}")
        conn.rollback()
    finally:
        cursor.close()

def handle_sentiment(msg_value, conn):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE live_news
            SET sentiment = %s, confidence_score = %s, reasoning = %s
            WHERE news_id = %s;
        """, (
            msg_value["sentiment"],
            msg_value.get("confidence_score", 0.0),
            msg_value.get("reasoning", ""),
            msg_value["news_id"]
        ))
        conn.commit()
    except Exception as e:
        logger.error(f"Error updating sentiment: {e}")
        conn.rollback()
    finally:
        cursor.close()

async def consume_and_store():
    consumer = AIOKafkaConsumer(
        RAW_NEWS_TOPIC,
        SENTIMENT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="storage-group",
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    
    await consumer.start()
    logger.info("Storage Worker listening for raw-news and sentiment-results...")

    # Dedicated Sync connection for PG in a separate coroutine or block
    # In production, psycopg2 should be run via asyncio run_in_executor or asyncpg
    # For MVP, synchronous pg in async Kafka loop is acceptable due to low load and fast network.
    conn = get_db_connection()

    try:
        async for msg in consumer:
            logger.info(f"Storage Worker received message from {msg.topic}")
            if msg.topic == RAW_NEWS_TOPIC:
                handle_raw_news(msg.value, conn)
            elif msg.topic == SENTIMENT_TOPIC:
                handle_sentiment(msg.value, conn)
    finally:
        conn.close()
        await consumer.stop()

if __name__ == "__main__":
    try:
        asyncio.run(consume_and_store())
    except KeyboardInterrupt:
        logger.info("Shutting down storage worker natively.")
