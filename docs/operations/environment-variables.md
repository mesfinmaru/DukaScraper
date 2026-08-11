# Environment variables

| Name | Default | Used by | Description |
|---|---|---|---|
| `APP_ENV` | `docker` / `wsl` | API and all workers | Selects Docker or host-facing endpoint overrides |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | API, workers | Kafka bootstrap address |
| `REDIS_URL` | `redis://localhost:6379/0` | Recursive crawl service | Redis endpoint for Bloom dedup |
| `TOR_SOCKS5_PROXY` | `socks5://tor:9050` | Dark worker | Tor SOCKS5 proxy |
| `OLLAMA_BASE_URL` | `http://ollama:11434` | LLM worker | Ollama API base URL |
| `OLLAMA_MODEL` | `qwen2.5:14b` | LLM worker | Model used for intelligence extraction |
| `CH_HOST` | `clickhouse` | LLM worker | ClickHouse host |
| `CH_PORT` | `8123` | LLM worker | ClickHouse HTTP port |
| `CH_DATABASE` | `duka_scraper` | LLM worker | ClickHouse database |
| `CH_USER` | `default` | LLM worker | ClickHouse username |
| `CH_PASSWORD` | empty | LLM worker | ClickHouse password |
| `MINIO_ENDPOINT` | `localhost:9000` | Workers/API | MinIO endpoint |
| `MINIO_ROOT_USER` | `minioadmin` | Workers/API | MinIO root user |
| `MINIO_ROOT_PASSWORD` | `minioadmin` | Workers/API | MinIO root password |
| `MINIO_ACCESS_KEY` | `minioadmin` | Worker settings | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | Worker settings | MinIO secret key |
| `MINIO_SECURE` | `False` | Workers/API | Whether to use HTTPS for MinIO |
| `POSTGRES_USER` | `postgres` | API/workers | PostgreSQL user |
| `POSTGRES_PASSWORD` | `postgres` | API/workers | PostgreSQL password |
| `POSTGRES_DB` | `duka` | API/workers | PostgreSQL database |
| `POSTGRES_HOST` | `localhost` | API/workers | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | API/workers | PostgreSQL port |
| `CLICKHOUSE_HOST` | `localhost` | Settings | ClickHouse host alias |
| `CLICKHOUSE_HTTP_PORT` | `8123` | Settings | ClickHouse HTTP port |
| `CLICKHOUSE_NATIVE_PORT` | `9002` | Settings | ClickHouse TCP port |
| `CLICKHOUSE_USER` | `default` | Settings | ClickHouse user |
| `CLICKHOUSE_PASSWORD` | empty | Settings | ClickHouse password |
| `CLICKHOUSE_DB` | `duka_scraper` | Settings | ClickHouse database |
