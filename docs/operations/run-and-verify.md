# Run and verify the pipeline

## 1. Start the stack

```bash
docker compose up -d
```

Verify the services are healthy:

```bash
docker compose ps
```

## 2. Trigger a job through the API

```bash
curl -X POST http://localhost:8000/api/v1/jobs/trigger \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"USR12345","url":"https://www.bbc.com/amharic","language":"am","max_depth":2,"recursive_config":{"enable_extraction":true}}'
```

## 3. Trigger a job through Kafka directly

```bash
python - <<'PY'
import json
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers=['localhost:29092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
producer.send('crawl.requests', {'job_id':'crawl_api_check','url':'https://www.dw.com/am/','language':'am','worker_type':'surface','job_params':{},'depth':0,'max_depth':2,'parent_url':None,'target_layer':'surface','recursive_config':{'enable_extraction':True}})
producer.flush()
PY
```

## 4. Verify the crawling workers

```bash
docker logs surface-worker --tail 50 2>&1 | Select-String "Processing job"
docker logs deep-worker --tail 50 2>&1 | Select-String "Rendering job"
docker logs dark-worker --tail 50 2>&1 | Select-String "Processing job"
```

## 5. Verify parsing

```bash
docker logs parser-worker --tail 50 2>&1 | Select-String "Produced parsed item"
```

## 6. Verify export and storage

```bash
curl -s http://localhost:9200/duka_articles/_search?size=3
```

```bash
docker exec postgres psql -U postgres -d duka -c "SELECT item_id, job_id, source_url, language, character_count FROM parsed_items ORDER BY parsed_at DESC LIMIT 5;"
```

```bash
docker exec clickhouse clickhouse-client --query "SELECT item_id, job_id, category, threat_severity, summary FROM duka_scraper.intelligence_analytics ORDER BY created_at DESC LIMIT 5;"
```

## 7. Verify recursive crawling

Check the worker logs for child requests with a depth greater than zero:

```bash
docker logs surface-worker --tail 80 2>&1 | Select-String "depth="
```

## 8. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Ollama responses are empty or the worker exits | The model has not been pulled yet | Run `docker exec ollama ollama pull qwen2.5:14b` |
| Dark worker never processes jobs | Tor is not healthy | Check `docker compose ps tor` and `docker logs tor --tail 50` |
| No events on Kafka topics | Topics were not created or the cluster is still booting | Wait for `kafka-topics-init` and re-run `docker compose ps` |
| Worker restarts repeatedly | The container is crashing on startup | Inspect `docker logs <worker-name> --tail 50` |
