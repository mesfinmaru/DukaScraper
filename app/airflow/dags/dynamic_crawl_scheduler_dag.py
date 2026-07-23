import json
from datetime import datetime
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

# Define the path to the JSON file with crawl targets
# This assumes the DAG file is in AIRFLOW_HOME/dags
DAGS_FOLDER = Path(__file__).parent
CRAWL_TARGETS_FILE = DAGS_FOLDER / "crawl_targets.json"

with DAG(
    dag_id="dynamic_crawl_scheduler",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["scraping", "dynamic"],
    doc_md="""
    ### Dynamic Crawl Scheduler DAG

    This DAG dynamically creates tasks to trigger crawl jobs based on a JSON configuration file.
    It reads `crawl_targets.json` and creates a `SimpleHttpOperator` task for each enabled target.
    """,
) as dag:
    # Load the crawl targets from the JSON file
    try:
        with open(CRAWL_TARGETS_FILE) as f:
            targets = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        targets = []

    # Dynamically create a task for each enabled target
    for target in targets:
        if target.get("enabled", False):
            task_id = f"trigger_crawl_{target['id']}"

            payload = {
                "url": target["url"],
                "render_js": target.get("render_js", False),
                "language": target.get("language", "en"),
            }

            SimpleHttpOperator(
                task_id=task_id,
                http_conn_id="duka_scraper_api",  # This connection must be configured in Airflow UI
                endpoint="/api/v1/jobs/trigger",
                method="POST",
                json=payload,
                headers={"Content-Type": "application/json"},
                response_check=lambda response: response.status_code == 202,
            )
