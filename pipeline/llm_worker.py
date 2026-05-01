import os
import json
import logging
import asyncio
import aiohttp
from pathlib import Path
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from dotenv import load_dotenv
from datetime import datetime, timezone

# VADER — Pass 1 (instant preliminary results)
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv(dotenv_path=Path(__file__).parent / ".env")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
LLM_MODEL = os.getenv("LLM_MODEL", "llama3.2:1b")
PROCESSED_NEWS_TOPIC = "processed-news"
SENTIMENT_TOPIC = "sentiment-results"

# ─── Settings ─────────────────────────────────────────────────────────────────
LLM_CONCURRENCY_LIMIT = int(os.getenv("LLM_CONCURRENCY_LIMIT", "1"))
LLM_TIMEOUT_SECONDS = int(os.getenv("LLM_TIMEOUT_SECONDS", "300"))
MAX_RETRIES = int(os.getenv("LLM_MAX_RETRIES", "3"))

vader = SentimentIntensityAnalyzer()

# ─── LLaMA System Prompt (includes VADER context hint) ────────────────────────
SYSTEM_PROMPT = """You are an expert financial news sentiment analyst.
For each headline, you will be given a VADER pre-analysis score as context.
Use it as a starting hint, but apply your own financial domain knowledge for the final verdict.

Respond ONLY with a valid JSON object with a "results" array. Each item MUST have:
- "news_id": exact string ID provided
- "sentiment": exactly one of "positive", "negative", or "neutral"
- "confidence_score": float 0.0 to 1.0
- "reasoning": one sentence explaining your classification

Example:
{
  "results": [
    {"news_id": "abc123", "sentiment": "positive", "confidence_score": 0.94, "reasoning": "Record earnings beat analyst expectations by a wide margin."},
    {"news_id": "def456", "sentiment": "negative", "confidence_score": 0.91, "reasoning": "CEO resignation amid fraud allegations signals deep corporate instability."}
  ]
}"""


def vader_score(title: str) -> dict:
    """Run VADER on a headline. Returns scores dict with compound and label."""
    scores = vader.polarity_scores(title)
    compound = scores["compound"]
    if compound >= 0.05:
        label = "positive"
    elif compound <= -0.05:
        label = "negative"
    else:
        label = "neutral"
    confidence = round(min(abs(compound) + 0.30, 0.65), 3)
    return {"compound": compound, "label": label, "confidence": confidence}


def build_batch_prompt(batch: list, vader_scores: dict) -> str:
    """Build LLaMA prompt embedding VADER pre-scores as context hints."""
    lines = ["Analyze these financial news headlines:\n"]
    for item in batch:
        nid = item["news_id"]
        vs = vader_scores.get(nid, {})
        vader_hint = (
            f"VADER hint: compound={vs.get('compound', 0):.3f} "
            f"({vs.get('label', 'unknown')})"
        )
        lines.append(
            f"ID: {nid} | Ticker: {item['company_ticker']} | "
            f"{vader_hint} | Headline: {item['title']}"
        )
    return "\n".join(lines)


async def call_ollama(session: aiohttp.ClientSession, prompt: str) -> dict:
    """Send prompt to local Ollama. Returns parsed JSON dict."""
    payload = {
        "model": LLM_MODEL,
        "prompt": prompt,
        "system": SYSTEM_PROMPT,
        "stream": False,
        "format": "json",
    }
    async with session.post(
        f"{OLLAMA_BASE_URL}/api/generate",
        json=payload,
        timeout=aiohttp.ClientTimeout(total=LLM_TIMEOUT_SECONDS),
    ) as resp:
        resp.raise_for_status()
        data = await resp.json()
        return json.loads(data.get("response", "{}"))


async def publish_result(producer, result: dict):
    """Publish a sentiment result to Kafka. Storage worker will UPSERT into DB."""
    event = {
        "news_id": result["news_id"],
        "company_ticker": result["company_ticker"],
        "sentiment": result["sentiment"],
        "confidence_score": result["confidence_score"],
        "reasoning": result.get("reasoning", ""),
        "llm_model": result.get("llm_model", LLM_MODEL),
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
    }
    await producer.send_and_wait(SENTIMENT_TOPIC, json.dumps(event).encode("utf-8"))


llama_queue = asyncio.Queue()

async def llama_refinement_worker(session, producer):
    while True:
        try:
            batch, vader_scores = await llama_queue.get()
            
            prompt = build_batch_prompt(batch, vader_scores)
            batch_ids = [item["news_id"] for item in batch]

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    start = datetime.now(timezone.utc)
                    logger.info(
                        f"🤖 PASS 2 — LLaMA [{LLM_MODEL}] refining {len(batch)} articles "
                        f"(attempt {attempt}/{MAX_RETRIES})..."
                    )
                    result_obj = await call_ollama(session, prompt)
                    elapsed = (datetime.now(timezone.utc) - start).total_seconds()

                    # Parse LLM response
                    if isinstance(result_obj, dict) and "results" in result_obj:
                        parsed = result_obj["results"]
                    elif isinstance(result_obj, list):
                        parsed = result_obj
                    else:
                        raise ValueError(f"Unexpected LLM response structure: {type(result_obj)}")

                    result_map = {
                        str(r.get("news_id")): r
                        for r in parsed
                        if isinstance(r, dict) and "news_id" in r
                    }

                    missing = [nid for nid in batch_ids if str(nid) not in result_map]
                    if missing:
                        logger.warning(
                            f"LLaMA omitted {len(missing)}/{len(batch)} articles "
                            f"(VADER results stay for these)"
                        )

                    # Overwrite VADER results with LLaMA results
                    refined = 0
                    for item in batch:
                        nid = str(item["news_id"])
                        if nid in result_map:
                            res = result_map[nid]
                            await publish_result(producer, {
                                "news_id": nid,
                                "company_ticker": item["company_ticker"],
                                "sentiment": res.get("sentiment", "neutral"),
                                "confidence_score": float(res.get("confidence_score", 0.0)),
                                "reasoning": res.get("reasoning", ""),
                                "llm_model": LLM_MODEL,
                            })
                            refined += 1

                    logger.info(
                        f"✅ PASS 2 done — LLaMA refined {refined}/{len(batch)} articles "
                        f"in {elapsed:.1f}s ({elapsed/len(batch):.1f}s per article). "
                        f"{len(batch) - refined} kept VADER result."
                    )
                    break  # Success — exit retry loop

                except asyncio.TimeoutError:
                    logger.warning(
                        f"⏱️  LLaMA timeout on attempt {attempt}/{MAX_RETRIES}. "
                        f"VADER Pass 1 results remain on dashboard."
                    )
                except (json.JSONDecodeError, ValueError) as e:
                    logger.error(f"LLaMA bad JSON on attempt {attempt}/{MAX_RETRIES}: {e}")
                except Exception as e:
                    logger.error(f"LLaMA error on attempt {attempt}/{MAX_RETRIES}: {e}")

                if attempt < MAX_RETRIES:
                    backoff = 2 ** attempt
                    logger.info(f"Retrying LLaMA in {backoff}s...")
                    await asyncio.sleep(backoff)
            
            llama_queue.task_done()
        except Exception as e:
            logger.error(f"Critical error in LLaMA Queue Worker: {e}")
            await asyncio.sleep(5)


async def analyze_batch(batch: list, producer):
    """
    Two-pass pipeline for every article batch:

    PASS 1 — VADER (instant, <1ms total)
      → Publishes preliminary results to DB immediately
      → Dashboard shows sentiment RIGHT NOW

    PASS 2 — LLaMA (accurate, ~30-40s) pushes to queue
      → Overwrites VADER results via DB UPSERT later
    """
    if not batch:
        return

    # ── PASS 1: VADER — instant preliminary results ────────────────────────
    vader_scores = {}
    logger.info(f"⚡ PASS 1 — VADER scoring {len(batch)} articles instantly...")

    for item in batch:
        vs = vader_score(item.get("title", ""))
        vader_scores[item["news_id"]] = vs
        await publish_result(producer, {
            "news_id": item["news_id"],
            "company_ticker": item["company_ticker"],
            "sentiment": vs["label"],
            "confidence_score": vs["confidence"],
            "reasoning": (
                f"VADER preliminary: compound={vs['compound']:.3f}. "
                f"Awaiting LLaMA refinement..."
            ),
            "llm_model": "VADER-preliminary",
        })

    logger.info(
        f"✅ PASS 1 done — {len(batch)} articles visible in dashboard immediately. "
        f"Queued for LLaMA refinement..."
    )

    # ── PASS 2: Queue for LLaMA ─────────────────────────────
    llama_queue.put_nowait((batch, vader_scores))


async def run_llm_worker():
    consumer = AIOKafkaConsumer(
        PROCESSED_NEWS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id="llm-worker-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)

    await consumer.start()
    await producer.start()

    logger.info(
        f"🚀 Two-Pass Sentiment Worker started.\n"
        f"   Pass 1 : VADER  → instant preliminary results (always runs)\n"
        f"   Pass 2 : LLaMA [{LLM_MODEL}] → accurate overwrite (~{LLM_TIMEOUT_SECONDS}s max)\n"
        f"   Fallback: VADER Pass 1 results stay if LLaMA fails\n"
        f"   Batch  : 10 articles | Timeout: {LLM_TIMEOUT_SECONDS}s"
    )

    async with aiohttp.ClientSession() as session:
        # Spawn background workers for LLaMA Pass 2
        for _ in range(LLM_CONCURRENCY_LIMIT):
            asyncio.create_task(llama_refinement_worker(session, producer))
            
        try:
            while True:
                try:
                    records = await consumer.getmany(timeout_ms=5000, max_records=10)
                except Exception as e:
                    logger.error(f"Kafka polling error: {e}")
                    await asyncio.sleep(2)
                    continue

                if not records:
                    continue

                batch = []
                for tp, msgs in records.items():
                    for msg in msgs:
                        try:
                            batch.append(json.loads(msg.value.decode("utf-8")))
                        except json.JSONDecodeError:
                            logger.error("Skipping undecodable Kafka message.")

                if batch:
                    await analyze_batch(batch, producer)
                    await consumer.commit()

        finally:
            await consumer.stop()
            await producer.stop()
            logger.info("Two-Pass Sentiment Worker shut down cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(run_llm_worker())
    except KeyboardInterrupt:
        logger.info("LLM Worker shutting down.")
