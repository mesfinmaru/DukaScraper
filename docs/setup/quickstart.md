# Quickstart

## Start the stack

```bash
docker compose up -d
```

## Wait for health

```bash
docker compose ps
```

Healthy services should include: `postgres`, `kafka`, `kafka-topics-init`, `elasticsearch`, `clickhouse`, `minio`, `ollama`, `api`, `surface-worker`, `parser-worker`, `deep-worker`, and `dark-worker`.

## Pull the Ollama model

```bash
docker exec ollama ollama pull qwen2.5:14b
```

## Access URLs

- Kafka UI: http://localhost:8088
- MinIO Console: http://localhost:9001
- pgAdmin: http://localhost:5050
- Kibana: http://localhost:5601
- ClickHouse HTTP: http://localhost:8123

Credentials:
- pgAdmin: `admin@example.com` / `admin`
- MinIO: `minioadmin` / `minioadmin`
