import os
import logging
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path=Path(__file__).parent / ".env")

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

    # Create Historical News Table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS historical_news (
            news_id VARCHAR(255) PRIMARY KEY,
            source_name VARCHAR(255) NOT NULL,
            company_ticker VARCHAR(50) NOT NULL,
            title TEXT NOT NULL,
            link TEXT,
            published_at TIMESTAMP WITH TIME ZONE,
            ingested_at TIMESTAMP WITH TIME ZONE,
            sentiment VARCHAR(50),
            confidence_score FLOAT,
            reasoning TEXT
        );
    """)

    # We use a single table "live_news" since the sentiment uniquely maps to the news_id
    # Create Indexes for massively accelerated analytical counting & sequential extraction
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_live_news_ingested_at ON live_news (ingested_at DESC);
        CREATE INDEX IF NOT EXISTS idx_live_news_composite ON live_news (company_ticker, sentiment);
    """)

    logger.info("Database schema initialized and securely indexed.")
    conn.commit()
    cursor.close()
    conn.close()

def clear_old_data():
    conn = get_db_connection()
    cursor = conn.cursor()

    logger.info("Archiving and clearing old data (previous day) from live database...")

    cursor.execute("""
        INSERT INTO historical_news
        SELECT * FROM live_news
        WHERE DATE(ingested_at AT TIME ZONE 'UTC') < CURRENT_DATE
        ON CONFLICT (news_id) DO NOTHING;
    """)
    archived_rows = cursor.rowcount

    cursor.execute("""
        DELETE FROM live_news 
        WHERE DATE(ingested_at AT TIME ZONE 'UTC') < CURRENT_DATE;
    """)
    deleted_rows = cursor.rowcount
    
    logger.info(f"Archived {archived_rows} records and removed {deleted_rows} stale records from live.")
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    init_db()
    clear_old_data()
