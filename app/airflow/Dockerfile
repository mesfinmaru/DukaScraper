# Custom Airflow image to include necessary providers
FROM apache/airflow:2.9.3-python3.12

# Switch to root to install packages, then back to airflow user
USER root
RUN apt-get update && apt-get install -y --no-install-recommends build-essential && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow

# Install the Postgres and HTTP providers.
# These packages include the necessary drivers (psycopg2) and operators (SimpleHttpOperator).
RUN pip install --no-cache-dir apache-airflow-providers-postgres apache-airflow-providers-http