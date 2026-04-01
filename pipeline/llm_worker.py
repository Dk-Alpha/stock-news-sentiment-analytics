import os
import json
import logging
import asyncio
import aiohttp
from pathlib import Path
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from dotenv import load_dotenv
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path=Path(__file__).parent / ".env")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
LLM_MODEL = os.getenv("LLM_MODEL", "llama3.2:latest")
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
Your task is to analyze the sentiment of a batch of news headlines. \
Respond ONLY with a valid JSON object containing a "results" array. Each object in the array MUST contain:
- "news_id": the exact string ID of the news item provided
- "sentiment": one of "positive", "negative", or "neutral"
- "confidence_score": a float between 0.0 and 1.0
- "reasoning": a single sentence explaining why

Example response:
{
  "results": [
    {"news_id": "123", "sentiment": "positive", "confidence_score": 0.91, "reasoning": "Strong earnings beat expectations."},
    {"news_id": "124", "sentiment": "negative", "confidence_score": 0.85, "reasoning": "Weak guidance for the next quarter."}
  ]
}
"""


def build_batch_prompt(batch: list) -> str:
    prompt = "Analyze the following news items:\n\n"
    for item in batch:
        prompt += f"ID: {item['news_id']} | Company: {item['company_ticker']} | Headline: {item['title']}\n"
    return prompt


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
    async with session.post(
        url, json=payload, timeout=aiohttp.ClientTimeout(total=LLM_TIMEOUT_SECONDS)
    ) as resp:
        resp.raise_for_status()
        data = await resp.json()
        raw_response = data.get("response", "{}")
        return json.loads(raw_response)


async def analyze_batch_with_retry(session, batch: list, producer, semaphore):
    if not batch:
        return
    prompt = build_batch_prompt(batch)
    batch_ids = [item["news_id"] for item in batch]

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with semaphore:
                start = datetime.now(timezone.utc)
                logger.info(
                    f"LLM analyzing batch of {len(batch)} items (attempt {attempt})..."
                )
                result_array = await call_ollama(session, prompt)
                elapsed_ms = int(
                    (datetime.now(timezone.utc) - start).total_seconds() * 1000
                )

            if not isinstance(result_array, dict) or "results" not in result_array:
                # Some implementations might return the unwrapped array regardless
                if isinstance(result_array, list):
                    parsed_array = result_array
                else:
                    raise ValueError(
                        "LLM response is not a JSON object containing 'results'"
                    )
            else:
                parsed_array = result_array["results"]

            # Map results by news_id
            result_map = {
                str(res.get("news_id")): res
                for res in parsed_array
                if isinstance(res, dict) and "news_id" in res
            }

            # Ensure all IDs were scored
            missing_ids = [nid for nid in batch_ids if str(nid) not in result_map]
            if missing_ids:
                logger.warning(f"LLM omitted {len(missing_ids)} items: {missing_ids}")
                # We will still process the ones that *were* scored

            # Publish successful results
            publish_tasks = []
            for item in batch:
                nid = str(item["news_id"])
                if nid in result_map:
                    res = result_map[nid]
                    sentiment_event = {
                        "news_id": nid,
                        "company_ticker": item["company_ticker"],
                        "sentiment": res.get("sentiment", "neutral"),
                        "confidence_score": res.get("confidence_score", 0.0),
                        "reasoning": res.get("reasoning", ""),
                        "llm_model": LLM_MODEL,
                        "analysis_time_ms": int(
                            elapsed_ms / len(batch)
                        ),  # Distribute time
                        "analyzed_at": datetime.now(timezone.utc).isoformat(),
                    }
                    publish_tasks.append(
                        producer.send_and_wait(
                            SENTIMENT_TOPIC, json.dumps(sentiment_event).encode("utf-8")
                        )
                    )

            if publish_tasks:
                await asyncio.gather(*publish_tasks)
            logger.info(
                f"✓ Sentiments extracted for {len(publish_tasks)}/{len(batch)} items in {elapsed_ms}ms"
            )

            return  # Success — exit retry loop

        except asyncio.TimeoutError:
            logger.warning(
                f"LLM timeout on attempt {attempt}/{MAX_RETRIES} for batch size {len(batch)}"
            )
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(
                f"LLM returned invalid JSON on attempt {attempt}/{MAX_RETRIES}: {e}"
            )
        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt}/{MAX_RETRIES}: {e}")

        if attempt < MAX_RETRIES:
            backoff = 2**attempt
            logger.info(f"Retrying batch in {backoff}s...")
            await asyncio.sleep(backoff)

    # All retries exhausted — send to Dead Letter Queue
    logger.error(f"All retries exhausted for batch of {len(batch)}. Sending to DLQ.")
    dlq_tasks = []
    for item in batch:
        dlq_event = {
            "news_id": item["news_id"],
            "company_ticker": item["company_ticker"],
            "title": item["title"],
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "reason": "LLM inference failed for batch after max retries",
        }
        dlq_tasks.append(
            producer.send_and_wait(DLQ_TOPIC, json.dumps(dlq_event).encode("utf-8"))
        )
    if dlq_tasks:
        await asyncio.gather(*dlq_tasks)


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
    logger.info(
        f"LLM Worker started. Model={LLM_MODEL} | Concurrency={LLM_CONCURRENCY_LIMIT} | Timeout={LLM_TIMEOUT_SECONDS}s"
    )

    semaphore = asyncio.Semaphore(LLM_CONCURRENCY_LIMIT)

    async with aiohttp.ClientSession() as session:
        try:
            while True:
                # Poll Kafka for a batch of messages
                try:
                    records = await consumer.getmany(timeout_ms=5000, max_records=10)
                except Exception as e:
                    logger.error(f"Kafka polling error: {e}")
                    await asyncio.sleep(2)
                    continue

                if not records:
                    continue  # No messages in this poll

                batch = []
                for tp, msgs in records.items():
                    for msg in msgs:
                        try:
                            batch.append(json.loads(msg.value.decode("utf-8")))
                        except json.JSONDecodeError:
                            logger.error("Could not decode Kafka message. Skipping.")

                if batch:
                    await analyze_batch_with_retry(session, batch, producer, semaphore)
                    # Only commit offset AFTER full batch processing is complete
                    await consumer.commit()

        finally:
            await consumer.stop()
            await producer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(run_llm_worker())
    except KeyboardInterrupt:
        logger.info("LLM Worker shutting down.")
