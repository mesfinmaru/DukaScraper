# Intelligence analytics

The LLM worker consumes parsed content from the `crawl.parsed` topic and writes intelligence analytics to ClickHouse. The supported categories are defined in [app/common/constants/intelligence_categories.py](../../app/common/constants/intelligence_categories.py):

- `data_leak`
- `gov_issue`
- `cyber_threat`
- `physical_threat`
- `misinformation`
- `other`

The Ollama prompt asks the local `qwen2.5:14b` model to return a compact JSON object containing the category, threat severity, entities, and a short summary. The ClickHouse ingestion path writes a row into `intelligence_analytics` with the job id, item id, URL, category, threat severity, entities, summary, and model metadata. Parser worker performance is also tracked in ClickHouse `crawler_performance` during parsing to support downstream monitoring and analytics.
