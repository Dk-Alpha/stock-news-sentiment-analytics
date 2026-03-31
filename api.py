import os
import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from typing import Optional
import asyncio
import json
import time

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="News Intelligence API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        database=os.getenv("POSTGRES_DB", "news_pipeline"),
        user=os.getenv("POSTGRES_USER", "pipeline_user"),
        password=os.getenv("POSTGRES_PASSWORD", "pipeline_pass"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )

# ─── Models ──────────────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str

# ─── Routes ──────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse)
def health():
    return {"status": "ok"}

@app.get("/news")
def get_news(ticker: Optional[str] = None, sentiment: Optional[str] = None, limit: int = 100):
    """Returns today's news, optionally filtered by ticker or sentiment."""
    conn = get_db()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        query = """
            SELECT news_id, source_name, company_ticker, title, link,
                   published_at, ingested_at, sentiment, confidence_score, reasoning
            FROM live_news
            WHERE DATE(ingested_at AT TIME ZONE 'UTC') = CURRENT_DATE
        """
        params = []
        if ticker:
            query += " AND company_ticker = %s"
            params.append(ticker.upper())
        if sentiment:
            query += " AND sentiment = %s"
            params.append(sentiment.lower())
        query += " ORDER BY ingested_at DESC LIMIT %s"
        params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Error fetching news: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.get("/news/summary")
def get_summary(ticker: Optional[str] = None):
    """Returns a sentiment summary breakdown for today's news."""
    conn = get_db()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        query = """
            SELECT company_ticker,
                   COUNT(*) AS total,
                   SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive,
                   SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative,
                   SUM(CASE WHEN sentiment = 'neutral'  THEN 1 ELSE 0 END) AS neutral,
                   AVG(confidence_score) AS avg_confidence
            FROM live_news
            WHERE DATE(ingested_at AT TIME ZONE 'UTC') = CURRENT_DATE
              AND sentiment IS NOT NULL
        """
        params = []
        if ticker:
            query += " AND company_ticker = %s"
            params.append(ticker.upper())
        query += " GROUP BY company_ticker ORDER BY total DESC"

        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

@app.get("/news/stream")
async def stream_news(ticker: Optional[str] = None):
    """Server-Sent Events endpoint for live UI streaming."""
    async def event_generator():
        seen_ids = set()
        while True:
            try:
                conn = get_db()
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                query = """
                    SELECT news_id, company_ticker, title, sentiment, confidence_score, ingested_at
                    FROM live_news
                    WHERE DATE(ingested_at AT TIME ZONE 'UTC') = CURRENT_DATE
                """
                params = []
                if ticker:
                    query += " AND company_ticker = %s"
                    params.append(ticker.upper())
                query += " ORDER BY ingested_at DESC LIMIT 50"
                cursor.execute(query, params)
                rows = cursor.fetchall()
                cursor.close()
                conn.close()

                for row in rows:
                    nid = row["news_id"]
                    if nid not in seen_ids:
                        seen_ids.add(nid)
                        yield f"data: {json.dumps(dict(row), default=str)}\n\n"
            except Exception:
                pass
            await asyncio.sleep(5)

    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.get("/tickers")
def get_tracked_tickers():
    """Returns list of tickers that have data today."""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT DISTINCT company_ticker
            FROM live_news
            WHERE DATE(ingested_at AT TIME ZONE 'UTC') = CURRENT_DATE
            ORDER BY company_ticker;
        """)
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
