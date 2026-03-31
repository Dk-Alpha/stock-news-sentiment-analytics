# 🖥️ UI Module

This directory contains the desktop UI application for the Live News Intelligence Dashboard.

## Features

- **Dark-mode dashboard** with real-time news feed auto-refreshing every 8 seconds
- **Sentiment summary** sidebar with per-ticker breakdown and visual progress bars
- **Filter by ticker** dropdown
- **⚙ Manage Sources** — live add/remove RSS sources via `sources.yaml` without restarting anything
- **Status indicator** showing whether the backend API is reachable

## Setup & Run

```powershell
# Copy and configure environment
Copy-Item .env.example .env

# Install dependencies
pip install -r requirements.txt

# Start the UI (API must be running first)
python ui.py
```

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `API_BASE_URL` | `http://localhost:8000` | FastAPI backend address |

> The UI reads and writes `sources.yaml` from the sibling `pipeline/` directory.
> The pipeline API (`api.py`) must be running for the dashboard to work.
