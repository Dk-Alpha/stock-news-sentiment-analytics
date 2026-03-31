import os
import logging
import psycopg2
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)

def get_db_connection():
    load_dotenv()
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        database=os.getenv("POSTGRES_DB", "news_pipeline"),
        user=os.getenv("POSTGRES_USER", "pipeline_user"),
        password=os.getenv("POSTGRES_PASSWORD", "pipeline_pass"),
        port=os.getenv("POSTGRES_PORT", "5432")
    )

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()

    logger.info("Initializing Database Schema...")

    # Create Raw News Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS live_news (
            news_id VARCHAR(255) PRIMARY KEY,
            source_name VARCHAR(255) NOT NULL,
            company_ticker VARCHAR(50) NOT NULL,
            title TEXT NOT NULL,
            link TEXT,
            published_at TIMESTAMP WITH TIME ZONE,
            ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            sentiment VARCHAR(50),
            confidence_score FLOAT,
            reasoning TEXT
        );
    """)

    # We use a single table "live_news" since the sentiment uniquely maps to the news_id
    # We will just UPSERT the sentiment when it arrives from the LLM worker.

    logger.info("Database schema initialized.")
    conn.commit()
    cursor.close()
    conn.close()

def clear_old_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    logger.info("Clearing old data (previous day) from database...")

    cursor.execute("""
        DELETE FROM live_news 
        WHERE DATE(ingested_at AT TIME ZONE 'UTC') < CURRENT_DATE;
    """)
    deleted_rows = cursor.rowcount
    
    logger.info(f"Deleted {deleted_rows} stale records from previous days.")
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    init_db()
    clear_old_data()
