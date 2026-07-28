"""
Worker to consume from `raw.result`, parse data, and fan-out to all data stores.
"""

import json
from datetime import datetime, timezone
from urllib.parse import urlparse

# Assume these are implemented elsewhere
from app.dependencies import (
    get_kafka_consumer, get_kafka_producer,
    get_db_session, get_clickhouse_client, get_elasticsearch_client
)
from app.storage.minio.client import minio_client
from app.models.schemas import RawResult, ParsedArticle
from app.pipeline.topics import topics


def run_parser_worker():
    """
    A long-running process that parses raw data and writes to final destinations.
    """
    # consumer = get_kafka_consumer(topics.RAW_RESULT) # Consumes from the raw result topic
    # producer = get_kafka_producer() # For publishing to parsed.articles
    # minio = get_minio_client()
    # db = get_db_session()
    # ch_client = get_clickhouse_client()
    # es_client = get_elasticsearch_client()

    print("Parser worker started. Listening for raw results...")
    # for message in consumer:
    #     raw_result = RawResult.model_validate(json.loads(message.value))
    #     print(f"Parsing result for Job ID: {raw_result.job_id}, URL: {raw_result.url}")

    #     # 1. Fetch raw HTML from MinIO
    #     # response = minio_client.client.get_object(
    #     #      bucket_name=minio_client.RAW_ASSETS_BUCKET,
    #     #      object_name=raw_result.html_blob_path
    #     # )
    #     # raw_html_bytes = response.read()

    #     # 2. Parse the content (e.g., using BeautifulSoup)
    #     # soup = BeautifulSoup(raw_html_bytes, 'html.parser')
    #     parsed_article = ParsedArticle(
    #         job_id=raw_result.job_id, url=raw_result.url, domain=urlparse(raw_result.url).netloc,
    #         title=f"Mock Title for {raw_result.url}", body_text_en="Mock English body text.",
    #         crawled_at=raw_result.crawled_at, html_blob_path=raw_result.html_blob_path
    #     )

    #     # --- FAN-OUT TO DATA STORES ---

    #     # 3a. Write metadata to PostgreSQL (Data-Plane)
    #     # page_record = Page(url=parsed_article.url, title=parsed_article.title, ...)
    #     # db.add(page_record)
    #     # db.commit()

    #     # 3b. Write analytics event to ClickHouse
    #     # latency_ms = (datetime.now(timezone.utc) - raw_result.crawled_at).total_seconds() * 1000
    #     # ch_client.execute("INSERT INTO crawl_metrics VALUES", [{
    #     #     'timestamp': raw_result.crawled_at, 'url': raw_result.url,
    #     #     'domain': parsed_article.domain,
    #     #     'status_code': raw_result.status_code, 'latency_ms': 150
    #     # }])

    #     # 3c. Index document in Elasticsearch for full-text search
    #     # es_client.index(index="articles", id=str(parsed_article.article_id), document=parsed_article.model_dump())

    #     # 3d. Publish to final `parsed.articles` topic for other consumers (e.g., real-time alerts)
    #     # producer.send(topics.PARSED_ARTICLES, value=parsed_article.model_dump_json())
    #     print(f"Successfully parsed and stored article from {parsed_article.url}")