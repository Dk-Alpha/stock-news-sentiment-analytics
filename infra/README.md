# 🐳 Infra Module

This directory manages Docker-based infrastructure.

## Services

| Service | Port | Purpose |
|---|---|---|
| `zookeeper` | 2181 | Kafka coordination |
| `kafka` | 9092 (internal), 9093 (external) | Event broker |
| `postgres` | 5432 | Persistent relational storage |
| `ollama` | 11434 | Local LLM runner (llama3, phi3, etc.) |

## Starting the Stack

```powershell
# From the infra/ directory
docker-compose up -d

# Pull your chosen LLM (first time only)
docker exec -it ollama ollama pull llama3.2:latest
```

## Safe Shutdown (with DB backup)

```powershell
# From the project ROOT directory
.\infra\shutdown.ps1
```

This will:
1. Run `pg_dump` on the running Postgres container
2. Save a timestamped `.sql` file to `../backups/`
3. Run `docker-compose down`

## GPU Support for Ollama

To enable NVIDIA GPU passthrough for faster LLM inference, uncomment the `deploy` block in `docker-compose.yml`:

```yaml
deploy:
  resources:
    reservations:
      devices:
        - driver: nvidia
          count: 1
          capabilities: [gpu]
```
