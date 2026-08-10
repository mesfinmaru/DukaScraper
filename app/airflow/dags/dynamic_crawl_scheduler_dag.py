import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from app.schemas.scraper import CrawlRequest
from kafka import KafkaProducer

from app.common.config.settings import settings

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def load_and_dispatch_targets(**context):
    """Reads crawl targets from JSON and dispatches them to Kafka as CrawlRequests."""
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(dag_folder, "crawl_targets.json")

    if not os.path.exists(json_path):
        raise FileNotFoundError(f"Could not find crawl targets file at: {json_path}")

    # Using utf-8-sig to handle any BOM automatically
    with open(json_path, encoding="utf-8-sig") as f:
        targets = json.load(f)

    producer = KafkaProducer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    dispatched_count = 0
    for target in targets:
        # Validate data against your explicit Pydantic schema contract
        request_payload = CrawlRequest(
            job_id=target.get("job_id"),
            url=target.get("url"),
            worker_type=target.get("worker_type", "surface"),
            language=target.get("language", "en"),
            job_params=target.get("job_params", {}),
        )

        payload_dict = request_payload.model_dump() if hasattr(request_payload, "model_dump") else request_payload.dict()

        producer.send(settings.crawl_request_topic, payload_dict)
        dispatched_count += 1
        print(f"🚀 Dispatched target: {request_payload.job_id} [{request_payload.language}] -> {request_payload.url}")

    producer.flush()
    print(f"✅ Successfully dispatched {dispatched_count} crawl targets to Kafka topic '{settings.crawl_request_topic}'.")


with DAG(
    "duka_dynamic_crawl_pipeline",
    default_args=default_args,
    description="Dynamically loads crawl targets and pushes them to Kafka",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:
    dispatch_targets_task = PythonOperator(
        task_id="dispatch_json_crawl_targets",
        python_callable=load_and_dispatch_targets,
    )
