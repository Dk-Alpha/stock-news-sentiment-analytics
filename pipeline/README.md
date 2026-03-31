# 🔧 Pipeline Module

This directory contains all Kafka-based data flow services.

## Services

| Script | Kafka Topic In | Kafka Topic Out | Purpose |
|---|---|---|---|
| `ingester.py` | — | `raw-news` | Async RSS poller. Reads `sources.yaml` and deduplicates articles. |
| `stream_processor.py` | `raw-news` | `processed-news` | Cleans and normalizes article text. |
| `llm_worker.py` | `processed-news` | `sentiment-results` | Calls local Ollama LLM API for financial sentiment. Fault-tolerant with DLQ. |
| `storage_worker.py` | `raw-news`, `sentiment-results` | — | Persists all events into PostgreSQL. |
| `api.py` | — | — | FastAPI REST + SSE streaming API for the UI to consume. |

## News Sources Config

Edit `sources.yaml` to add or remove RSS feeds. Changes are picked up **automatically** on the next polling cycle — no restart required.

```yaml
sources:
  - name: GoogleNews_Apple
    url: "https://news.google.com/rss/search?q=Apple+stock"
    type: "rss"
    company_ticker: "AAPL"
```

## Setup & Run

```powershell
# Copy and configure environment
Copy-Item .env.example .env

# Install dependencies
pip install -r requirements.txt        # Kafka workers
pip install -r requirements_api.txt    # FastAPI backend

# Start workers (each in a separate terminal)
python ingester.py
python stream_processor.py
python llm_worker.py
python storage_worker.py
python api.py
```

## LLM Fault Tolerance

Controlled entirely via `.env`:

| Variable | Default | Purpose |
|---|---|---|
| `LLM_CONCURRENCY_LIMIT` | `1` | Max simultaneous Ollama calls (prevents CPU/GPU overload) |
| `LLM_TIMEOUT_SECONDS` | `60` | Per-request timeout before retry |
| `LLM_MAX_RETRIES` | `3` | Retries before sending to `dead-letter-queue` |
