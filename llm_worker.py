import os
import json
import logging
import asyncio
import aiohttp
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from dotenv import load_dotenv
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
LLM_MODEL = os.getenv("LLM_MODEL", "llama3:8b")
PROCESSED_NEWS_TOPIC = "processed-news"
SENTIMENT_TOPIC = "sentiment-results"
DLQ_TOPIC = "dead-letter-queue"

# ─── Fault Tolerance Settings ────────────────────────────────────────────────
# Max number of simultaneous LLM calls — protects your CPU/GPU from overload
LLM_CONCURRENCY_LIMIT = int(os.getenv("LLM_CONCURRENCY_LIMIT", "1"))
# Seconds to wait for Ollama before timing out and re-queuing the message
LLM_TIMEOUT_SECONDS = int(os.getenv("LLM_TIMEOUT_SECONDS", "60"))
# Number of retries before sending to Dead Letter Queue
MAX_RETRIES = int(os.getenv("LLM_MAX_RETRIES", "3"))

SYSTEM_PROMPT = """You are a financial news analyst. \
Your task is to analyze the sentiment of the given news headline for the specified company. \
Respond ONLY with a valid JSON object containing:
- "sentiment": one of "positive", "negative", or "neutral"
- "confidence_score": a float between 0.0 and 1.0
- "reasoning": a single sentence explaining why

Example response:
{"sentiment": "positive", "confidence_score": 0.91, "reasoning": "Strong earnings beat expectations significantly."}
"""

def build_prompt(ticker: str, title: str) -> str:
    return f"Company: {ticker}\nHeadline: {title}"

async def call_ollama(session: aiohttp.ClientSession, prompt: str) -> dict:
    """Calls the local Ollama API. Raises on timeout or error."""
    payload = {
        "model": LLM_MODEL,
        "prompt": prompt,
        "system": SYSTEM_PROMPT,
        "stream": False,
        "format": "json",
    }
    url = f"{OLLAMA_BASE_URL}/api/generate"
    async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=LLM_TIMEOUT_SECONDS)) as resp:
        resp.raise_for_status()
        data = await resp.json()
        raw_response = data.get("response", "{}")
        return json.loads(raw_response)

async def analyze_with_retry(session, news_item, producer, semaphore):
    ticker = news_item["company_ticker"]
    title = news_item["title"]
    news_id = news_item["news_id"]
    prompt = build_prompt(ticker, title)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore:
                start = datetime.now(timezone.utc)
                logger.info(f"LLM analyzing [{ticker}] attempt {attempt}: {title[:60]}...")
                result = await call_ollama(session, prompt)
                elapsed_ms = int((datetime.now(timezone.utc) - start).total_seconds() * 1000)

            sentiment_event = {
                "news_id": news_id,
                "company_ticker": ticker,
                "sentiment": result.get("sentiment", "neutral"),
                "confidence_score": result.get("confidence_score", 0.0),
                "reasoning": result.get("reasoning", ""),
                "llm_model": LLM_MODEL,
                "analysis_time_ms": elapsed_ms,
                "analyzed_at": datetime.now(timezone.utc).isoformat(),
            }

            await producer.send_and_wait(SENTIMENT_TOPIC, json.dumps(sentiment_event).encode("utf-8"))
            logger.info(f"✓ Sentiment [{ticker}]: {result.get('sentiment')} ({result.get('confidence_score', 0):.2f}) in {elapsed_ms}ms")
            return  # Success — exit retry loop

        except asyncio.TimeoutError:
            logger.warning(f"LLM timeout on attempt {attempt}/{MAX_RETRIES} for news_id={news_id}")
        except json.JSONDecodeError as e:
            logger.error(f"LLM returned invalid JSON on attempt {attempt}/{MAX_RETRIES}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt}/{MAX_RETRIES} for {news_id}: {e}")

        if attempt < MAX_RETRIES:
            backoff = 2 ** attempt
            logger.info(f"Retrying in {backoff}s...")
            await asyncio.sleep(backoff)

    # All retries exhausted — send to Dead Letter Queue
    logger.error(f"All retries exhausted for news_id={news_id}. Sending to DLQ.")
    dlq_event = {
        "news_id": news_id,
        "company_ticker": ticker,
        "title": title,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "reason": "LLM inference failed after max retries",
    }
    await producer.send_and_wait(DLQ_TOPIC, json.dumps(dlq_event).encode("utf-8"))

async def run_llm_worker():
    consumer = AIOKafkaConsumer(
        PROCESSED_NEWS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="llm-worker-group",
        auto_offset_reset="earliest",
        # CRITICAL: disable auto-commit so we control when offsets are committed
        enable_auto_commit=False,
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)

    await consumer.start()
    await producer.start()
    logger.info(f"LLM Worker started. Model={LLM_MODEL} | Concurrency={LLM_CONCURRENCY_LIMIT} | Timeout={LLM_TIMEOUT_SECONDS}s")

    semaphore = asyncio.Semaphore(LLM_CONCURRENCY_LIMIT)

    async with aiohttp.ClientSession() as session:
        try:
            async for msg in consumer:
                try:
                    news_item = json.loads(msg.value.decode("utf-8"))
                except json.JSONDecodeError:
                    logger.error("Could not decode message from Kafka. Skipping.")
                    await consumer.commit()
                    continue

                await analyze_with_retry(session, news_item, producer, semaphore)

                # Only commit offset AFTER processing is complete (at-least-once guarantee)
                await consumer.commit()

        finally:
            await consumer.stop()
            await producer.stop()

if __name__ == "__main__":
    try:
        asyncio.run(run_llm_worker())
    except KeyboardInterrupt:
        logger.info("LLM Worker shutting down.")
