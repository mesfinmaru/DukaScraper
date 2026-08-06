# DUKA SCRAPER - QUICK REFERENCE & DATA FLOW

## System Architecture at a Glance

```
┌─────────────────────────────────────────────────────────────────┐
│                    USER / AIRFLOW SCHEDULER                     │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
            ┌────────────────────────────┐
            │    FASTAPI GATEWAY         │
            │  (Auth + Job Management)   │
            └───────┬──────────┬─────────┘
                    │          │
        ┌──────────▼──┐   ┌──▼──────────────┐
        │ PostgreSQL  │   │  Kafka          │
        │(duka_system)│   │(crawl.requests) │
        │             │   │                 │
        │ users       │   └────────┬────────┘
        │ jobs        │            │
        └─────────────┘            ▼
                        ┌──────────────────────┐
                        │ WORKERS (Scalable)   │
                        │                      │
                        │ • Surface Worker     │
                        │ • Deep Worker        │
                        │ • Dark Worker        │
                        └─────────┬────────────┘
                                  │
                ┌─────────────────┼─────────────────┐
                ▼                 ▼                 ▼
           ┌────────────┐  ┌───────────┐  ┌──────────────┐
           │ MinIO      │  │ PostgreSQL│  │ Kafka        │
           │(duka_raw)  │  │(duka_db)  │  │(crawl.raw)   │
           │            │  │           │  │              │
           │Raw HTML    │  │Update job │  │Publish raw   │
           │files       │  │progress   │  │content       │
           └────────────┘  └───────────┘  └──────┬───────┘
                                                 │
                                                 ▼
                                        ┌──────────────────┐
                                        │  PARSER WORKER   │
                                        │                  │
                                        │ • Parse HTML     │
                                        │ • Extract text   │
                                        │ • Generate JSON  │
                                        └────────┬─────────┘
                    ┌───────────────────────────┼───────────────────────────┐
                    ▼                           ▼                           ▼
            ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
            │ MinIO            │      │ PostgreSQL       │      │ Kafka            │
            │ (duka_parsed)    │      │ (duka_db)        │      │ (crawl.parsed)   │
            │                  │      │                  │      │                  │
            │ Parsed JSON      │      │ parsed_items     │      │ Publish parsed   │
            │ files            │      │ exports          │      │ content (3x)     │
            └──────────────────┘      └──────────────────┘      └─────────┬────────┘
                                                                           │
                        ┌──────────────────────────────────────────────────┼──────────────────────────────────────┐
                        ▼                                                   ▼                                       ▼
            ┌─────────────────────────┐                        ┌─────────────────────────┐        ┌─────────────────────────┐
            │  ES INDEXER CONSUMER    │                        │  CH LOADER CONSUMER     │        │ EXPORTER CONSUMER       │
            │                         │                        │                         │        │                         │
            │ Consumes crawl.parsed   │                        │ Consumes crawl.parsed   │        │ Consumes crawl.parsed   │
            │ Indexes full-text       │                        │ Loads time-series data  │        │ Batches 1000 items      │
            └────────────┬────────────┘                        └────────────┬────────────┘        └────────────┬────────────┘
                         ▼                                                  ▼                                     ▼
            ┌─────────────────────────┐                        ┌─────────────────────────┐        ┌─────────────────────────┐
            │ Elasticsearch           │                        │ ClickHouse              │        │ MinIO                   │
            │ (duka_articles index)   │                        │                         │        │ (duka_exports)          │
            │                         │                        │ crawl_metrics           │        │                         │
            │ Full-text searchable    │                        │ parse_metrics           │        │ CSV/JSON/Parquet        │
            │ content                 │                        │ export_metrics          │        │ exports                 │
            └─────────────────────────┘                        └─────────────────────────┘        └─────────────────────────┘
```

---

## Data Storage by System

| System | Purpose | Data Type | Size | TTL |
|--------|---------|-----------|------|-----|
| **PostgreSQL** | Metadata + orchestration | Structured | ~260 MB/1M docs | Permanent |
| **MinIO (raw)** | Raw HTML archive | Binary (gzipped) | ~5 GB/1M docs | 90 days |
| **MinIO (parsed)** | Parsed JSON archive | Binary (gzipped) | ~2 GB/1M docs | 90 days |
| **MinIO (exports)** | User exports | Binary (gzipped) | ~500 MB | 7 days |
| **Elasticsearch** | Full-text search | Indexed text | ~2 GB/1M docs | 30 days |
| **ClickHouse** | Analytics metrics | Time-series | ~8 GB/1M docs | 90 days |

---

## ID Formats Generated

```
User ID:      USR12345 (Random 5-digit + USR prefix)
Job ID:       JOB00000001 (Sequence-based + JOB prefix)
Item ID:      ITEM00000001 (Sequence-based + ITEM prefix)
Export ID:    EXP00000001 (Sequence-based + EXP prefix)
```

---

## Complete Job Lifecycle

```
1. User/Airflow submits request
   │
   ├─→ FastAPI validates input
   │
   ├─→ PostgreSQL: INSERT jobs (status='pending')
   │
   └─→ Kafka: Publish to crawl.requests

2. Surface/Deep/Dark Worker consumes
   │
   ├─→ Fetch URL
   │
   ├─→ MinIO: Upload raw HTML (duka_raw)
   │
   ├─→ PostgreSQL: Update job progress
   │
   └─→ Kafka: Publish to crawl.raw

3. Parser Worker consumes
   │
   ├─→ Parse HTML (BeautifulSoup)
   │
   ├─→ MinIO: Upload parsed JSON (duka_parsed)
   │
   ├─→ PostgreSQL: INSERT parsed_items
   │
   └─→ Kafka: Publish to crawl.parsed (3 consumers)

4A. Elasticsearch Indexer
    │
    ├─→ Index full-text content
    │
    └─→ Users can search

4B. ClickHouse Loader
    │
    ├─→ Load performance metrics
    │
    └─→ Dashboards show analytics

4C. Export Worker
    │
    ├─→ Batch 1000 items
    │
    ├─→ Generate CSV/JSON/Parquet
    │
    ├─→ MinIO: Upload (duka_exports)
    │
    ├─→ PostgreSQL: INSERT exports
    │
    └─→ Users can download

5. User retrieves results
   │
   ├─→ Search via Elasticsearch
   │
   ├─→ Analytics via ClickHouse
   │
   └─→ Download via MinIO pre-signed URLs
```

---

## One-Minute Startup

```bash
# 1. Start all services
docker compose up -d

# 2. Wait for PostgreSQL
sleep 10

# 3. Initialize databases (already done)
# Tables are pre-created in duka_system and duka_db

# 4. Create Kafka topics
docker exec kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 --topic crawl.requests --partitions 10 --replication-factor 1

docker exec kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 --topic crawl.raw --partitions 10 --replication-factor 1

docker exec kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 --topic crawl.parsed --partitions 10 --replication-factor 1

docker exec kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 --topic export.requests --partitions 5 --replication-factor 1

# 5. Create MinIO buckets
docker exec minio mc alias set minio http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb minio/duka_raw
docker exec minio mc mb minio/duka_parsed
docker exec minio mc mb minio/duka_exports

# 6. Create Elasticsearch index
curl -X PUT "localhost:9200/duka_articles" \
  -H 'Content-Type: application/json' \
  -d '{"settings":{"number_of_shards":5,"number_of_replicas":1},"mappings":{"properties":{"item_id":{"type":"keyword"},"job_id":{"type":"keyword"},"source_url":{"type":"keyword"},"title":{"type":"text"},"extracted_text":{"type":"text"},"language":{"type":"keyword"},"publish_date":{"type":"date"},"source_domain":{"type":"keyword"},"indexed_at":{"type":"date"}}}}'

# 7. Ready to go!
echo "Pipeline ready. Visit:"
echo "- FastAPI: http://localhost:8000/docs"
echo "- Kafka UI: http://localhost:8088"
echo "- MinIO: http://localhost:9001"
echo "- Elasticsearch: http://localhost:9200"
echo "- ClickHouse: http://localhost:8123"
echo "- pgAdmin: http://localhost:5050"
```

---

## Test Sample Data

### 1. Insert Test User
```sql
INSERT INTO users (full_name, username, email, password_hash)
VALUES ('Test User', 'testuser', 'test@example.com', 'pwd_hash');
```

### 2. Submit Crawl Job
```bash
curl -X POST http://localhost:8000/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": "USR00001",
    "url": "https://www.bbc.com/amharic",
    "worker_type": "surface",
    "language": "am"
  }'
```

### 3. Publish to Kafka (Simulate Surface Worker)
```bash
echo '{
  "job_id": "JOB00000001",
  "source_url": "https://www.bbc.com/amharic",
  "raw_html": "<html>Test content</html>",
  "http_status_code": 200,
  "fetch_time_ms": 245,
  "content_size_bytes": 1024,
  "raw_html_s3_path": "s3://duka_raw/job_001/hash_abc.html.gz"
}' | docker exec -i kafka kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic crawl.raw
```

### 4. Search Results
```bash
curl -X GET "localhost:9200/duka_articles/_search?q=*"
```

### 5. Check Job Status
```bash
docker exec postgres psql -U postgres -d duka_system -c \
  "SELECT * FROM jobs LIMIT 1;"
```

---

## Performance Targets

| Metric | Target | Actual (Sample) |
|--------|--------|---|
| URLs/min (1 worker) | 240 | - |
| Docs/min (1 worker) | 400 | - |
| Avg crawl time | 250ms | - |
| Avg parse time | 150ms | - |
| Search latency | 50ms | - |
| Storage efficiency | 1.7 GB/1M | - |

---

## Architecture Benefits

✅ **Scalability**: Add workers horizontally (10+ workers = 10,000+ URLs/day)
✅ **Resilience**: Kafka buffers spikes, retry logic handles failures
✅ **Cost**: Compressed storage (~85% reduction)
✅ **Performance**: Parallel consumers (ES, ClickHouse, Export)
✅ **Flexibility**: MinIO, ES, ClickHouse independent
✅ **Monitoring**: All components exportable to Prometheus
✅ **Auditability**: Full job history in PostgreSQL

This is production-ready infrastructure for a distributed web scraping system.
