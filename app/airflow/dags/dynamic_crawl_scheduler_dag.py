from __future__ import annotations

import json
from pathlib import Path

import pendulum
from airflow.models.dag import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# Path to the JSON file containing the crawl targets
DAGS_FOLDER = Path(__file__).parent
TARGETS_FILE = DAGS_FOLDER / "crawl_targets.json"

with DAG(
    dag_id="dynamic_crawl_scheduler",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    doc_md="""
    ### Dynamic Crawl Scheduler DAG

    This DAG dynamically generates tasks based on a `crawl_targets.json` file.
    It runs on a schedule and triggers crawl jobs by calling the Duka Scraper API.

    **Connection Setup:**
    - You must create an Airflow HTTP Connection with `Conn Id` = `duka_api`.
    - The connection should point to the API service: `http://api:8000`.
    """,
    tags=["crawler", "scheduler", "dynamic"],
) as dag:
    # Load the targets from the JSON file
    with open(TARGETS_FILE) as f:
        targets = json.load(f)

    # Dynamically create a task for each enabled target
    for target in targets:
        if target.get("enabled", False):
            task_id = f"trigger_crawl_{target['id']}"

            # Include render_js so the API can route the job to the correct worker.
            payload = {
                "url": target["url"],
                "render_js": target.get("render_js", False),
            }

            SimpleHttpOperator(
                task_id=task_id,
                http_conn_id="duka_api",
                method="POST",
                endpoint="/api/v1/jobs/trigger",
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                response_check=lambda response: response.status_code == 202,
                log_response=True,
            )
