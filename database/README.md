# 🗄️ Database Module

This directory manages PostgreSQL schema initialization and daily data lifecycle.

## Scripts

| Script | Purpose |
|---|---|
| `db_init.py` | Creates schema (tables) and **deletes previous day's data** automatically on startup |

## Schema

Single table `live_news` holds both raw news and enriched sentiment in one row (UPSERT pattern):

| Column | Type | Description |
|---|---|---|
| `news_id` | VARCHAR PK | SHA-256 hash of title + link + published date |
| `source_name` | VARCHAR | Name of the RSS source |
| `company_ticker` | VARCHAR | Stock ticker (e.g. AAPL) |
| `title` | TEXT | Article headline |
| `link` | TEXT | Original article URL |
| `published_at` | TIMESTAMPTZ | Original publish time |
| `ingested_at` | TIMESTAMPTZ | When the ingester picked it up |
| `sentiment` | VARCHAR | `positive` / `negative` / `neutral` (filled by LLM worker) |
| `confidence_score` | FLOAT | LLM confidence 0.0–1.0 |
| `reasoning` | TEXT | One-sentence LLM explanation |

## Daily Clearance

On every `db_init.py` run, records with `ingested_at < CURRENT_DATE` are automatically deleted.  
This keeps the database lean — we only care about today's intraday news.

## Setup & Run

```powershell
# Copy and configure
Copy-Item .env.example .env

# Install dependencies
pip install -r requirements.txt

# Initialize schema and clear stale data
python db_init.py
```

## Database Backups

Backups are handled by `infra/shutdown.ps1`.  
Before Docker is stopped, it runs `pg_dump` and saves a timestamped `.sql` file to `../backups/`.
