# Duka Scraper — Distributed Amharic Web Crawling & Analytics Engine

## Section 1. System Overview & Objectives

**Duka Scraper** is a distributed, Amharic-focused web scraping and content extraction platform designed for high-throughput data collection from the surface, deep, and dark web.

### Core System Objectives

*   **Massive Scale**: Architected to crawl and process millions of pages by horizontally scaling stateless worker components.
*   **Dynamic Content Handling**: Capable of rendering and interacting with JavaScript-heavy websites using browser automation.
*   **Dark Web Capability**: Provides a secure, opt-in mechanism for crawling `.onion` sites via an isolated Tor proxy.
*   **Horizontal Worker Scaling**: All crawling, parsing, and exporting tasks are performed by independent worker pools that can be scaled up or down based on load.
*   **Multi-Engine Storage**: Utilizes a polyglot persistence strategy, selecting the best database technology (Transactional, Search, Analytics, Object) for each specific data type and access pattern.

## Section 2. High-Level System Architecture Diagram

The system is divided into two primary flows: the **Write/Crawl Pipeline** (left) for data ingestion and the **Read/Query Pipeline** (right) for data access.

```text
                                       +------------------------------------------------------------------+
                                       |               DUKA SCRAPER - SYSTEM ARCHITECTURE                 |
                                       +------------------------------------------------------------------+

  WRITE / CRAWL PIPELINE                                                          READ / QUERY PIPELINE
  ======================                                                          =====================

  +----------+   HTTP    +-----------+  1. Create Job  +------------+             +-----------+  2. Search  +---------------+
  | Airflow  |---------> |           |--------------> | PostgreSQL |             | React UI  |---------> |               |
  | Scheduler|           |           | (Job Metadata)  +------------+             | (Browser) |           |               |
  +----------+           |           |                                            +-----------+           |               |
                       |           |                                                                  |               |
  +----------+   HTTP    |  FastAPI  |  1. Dispatch    +------------------+                               |  FastAPI      |
  |   User   |---------> |  Gateway  |--------------> |      KAFKA       |                               |  Gateway      |
  +----------+           |           |               | crawl.requests   |                               |               |
                       |           |               +------------------+                               |               |
                       +-----+-----+                       |                                          |               |
                             |                             | 2. Consume Job                           |               |
                             |                             |                                          |               |
                             |         +-------------------+-------------------+                      |               |
                             |         |                   |                   |                      |               |
                             v         v                   v                   v                      |               |
                      +--------------+      +-------------+       +-------------+                     |               |
                      | Surface Worker |      | Deep Worker |       | Dark Worker |                     |               |
                      | (aiohttp)      |      | (Playwright)|       | (Tor Proxy) |                     |               |
                      +--------------+      +-------------+       +-------------+                     |               |
                             |                   |                   |                                |               |
                             | 3. Produce Raw HTML |                   |                                |               |
                             +-------------------+-------------------+                                |               |
                                                 |                                                    |               |
                                                 v                                                    |               |
                                       +------------------+                                           |               |
                                       |      KAFKA       |                                           |               |
                                       | raw.html.ingest  |                                           |               |
                                       +------------------+                                           |               |
                                                 |                                                    |               |
                                                 | 4. Consume Raw HTML                              +---------------+
                                                 v                                                    ^      ^      ^
                                       +-----------------+                                            |      |      |
                                       |  Parser Worker  |                                            |      |      |
                                       | (BeautifulSoup) |                                            |      |      |
                                       +-----------------+                                            |      |      |
                                                 | 5. Fan-out Parsed Data                             |      |      |
      +------------------------------------------+-----------------------------------------+           |      |      |
      |                                          |                                         |           |      |      |
      v                                          v                                         v           |      |      |
+------------------+                      +------------------+                      +------------------+  |      |      |
|      KAFKA       |                      |      MinIO       |                      |   ClickHouse     |  |      |      |
| parsed.articles  |                      | (Raw HTML/Exports)|                      | (Crawl Analytics)|  |      |      |
+------------------+                      +------------------+                      +------------------+  |      |      |
      |                                          ^                                         ^            |      |      |
      | 6. Consume for Search/Export             |                                         |            |      |      |
      v                                          |                                         |            |      |      |
+------------------+                      +------------------+                               |            |      |      |
|  Elasticsearch   |                      |  Export Worker   |                               |            |      |      |
| (Full-Text Index)|                      | (CSV/JSON/PDF)   |                               |            |      |      |
+------------------+                      +------------------+                               |            |      |      |
      ^                                                                                     |            |      |      |
      |                                                                                     |            |      |      |
      +-------------------------------------------------------------------------------------+------------+------+
                                                                                                        |
                                                                                                        +-------------> [User]

```

## Section 3. Presentation Layer

*   **Tech Stack**: React, Vite, Tailwind CSS, Axios
*   **Responsibilities & User Capabilities**:
    *   **Authentication**: Secure login and session management.
    *   **Job Management**: A UI for creating, configuring, and starting new crawl jobs.
    *   **Live Monitoring**: Dashboards to visualize crawl progress, worker status, and system health.
    *   **Full-Text Search**: An interface to query the extracted Amharic text content stored in Elasticsearch.
    *   **Data Export**: A download center where users can retrieve raw HTML or generated datasets (CSV, PDF) from MinIO.
    *   **Analytics Dashboards**: High-density charts and graphs powered by ClickHouse to analyze crawl performance and domain metrics.

## Section 4. API Gateway Layer

*   **Tech Stack**: FastAPI
*   **Responsibilities**:
    *   **Authentication & Authorization**: Validates user credentials and permissions for all incoming requests.
    *   **Job Dispatch**: Receives job requests, validates them, stores metadata in PostgreSQL, and dispatches them to the `crawl.requests` Kafka topic.
    *   **Search Proxy**: Acts as a secure backend-for-frontend (BFF) that translates simple user search queries into complex Elasticsearch DSL queries.
    *   **Export Generation**: Provides endpoints to trigger the creation of export files and to generate pre-signed URLs for secure, direct-from-client downloads from MinIO.
    *   **Health & Metrics**: Exposes `/health` and `/metrics` endpoints for service discovery and Prometheus scraping.
*   **Example Request**: `POST /api/v1/jobs`
    ```json
    {
      "url": "https://am.thereporter.et/",
      "worker_type": "surface",
      "language": "am",
      "job_params": {
        "render_js": false
      }
    }
    ```

## Section 5. Database & Storage Layer (Multi-Engine Storage Strategy)

*   **PostgreSQL (Transactional / Metadata)**: The primary system of record for structured, relational data.
    *   **Data**: User accounts, project settings, job definitions, worker status, and crawl metadata (e.g., page URL, status code, last-crawled timestamp).
    *   **Reason**: Ensures high consistency and transactional integrity for critical system state.
*   **ClickHouse (Time-Series / Analytics Engine)**: A high-throughput columnar database for large-scale analytics.
    *   **Data**: Time-series events from the pipeline, such as crawl request times, response latencies, content size, and worker processing durations.
    *   **Reason**: Optimized for extremely fast aggregations over massive datasets, perfect for powering real-time analytics dashboards on crawl velocity and domain performance.
*   **Elasticsearch (Full-Text Search Engine)**: A distributed search and analytics engine.
    *   **Data**: The structured, parsed Amharic text content (`parsed.articles`).
    *   **Reason**: Provides powerful and fast full-text search capabilities, including custom Amharic tokenization, fuzzy matching, and relevance scoring for the search UI.
*   **MinIO (S3-Compatible Object Storage)**: The data lake for all unstructured or semi-structured binary data.
    *   **Data**: Raw HTML payloads, browser screenshots, and generated export packages (PDF, CSV, JSON).
    *   **Reason**: Offers a scalable, durable, and cost-effective solution for storing large binary objects, accessible via a standard S3 API.

## Section 6. Messaging & Event Bus Layer (Apache Kafka)

*   **Kafka Role & Architecture Benefits**: Kafka serves as the central nervous system of the ingestion pipeline. It decouples producers (scrapers) from consumers (parsers, indexers), provides a durable buffer to handle processing spikes, and enables high-throughput, parallel processing of data streams.
*   **Defined Kafka Topics & Payloads**:
    1.  **`crawl.requests`**: Dispatches new jobs from the API to the available pool of crawling workers.
        *   *Payload*: `CrawlRequest` schema (job_id, url, worker_type, job_params).
    2.  **`raw.html.ingest`**: A stream of raw, unprocessed HTML content produced by the crawling workers.
        *   *Payload*: `CrawlResult` schema (source_job_id, url, html, status_code).
    3.  **`parsed.articles`**: A stream of structured, cleaned JSON data produced by the Parser Worker, ready for final storage and indexing.
        *   *Payload*: `ParsedItem` schema (source_job_id, url, language, data: {extracted_text, ...}).

## Section 7. Crawling & Processing Layer (Worker Microservices)

*   **Surface Worker**: Optimized for speed on static websites. Uses `aiohttp` or `httpx` for fast, asynchronous HTTP requests.
*   **Deep Worker**: Handles dynamic, JavaScript-rendered websites. Uses browser automation tools like `Playwright` or `Selenium` to execute JS, fill out forms, and handle complex user interactions. Also responsible for recursive link extraction and depth tracking.
*   **Dark Worker**: A specialized worker for crawling `.onion` domains. Routes all traffic through a dedicated Tor proxy service to ensure anonymity and access to the Tor network.
*   **Parser Worker**: Consumes raw HTML from `raw.html.ingest`. Uses `BeautifulSoup` to parse the DOM, clean the text, extract Amharic content and metadata, and produces structured JSON to the `parsed.articles` topic.
*   **Export Worker**: Listens to the `parsed.articles` topic (or is triggered via API) to generate bulk dataset packages (e.g., CSV, JSON, Excel) and saves them directly to a MinIO bucket for user download.

## Section 8. Search & Analytics Layer

*   **Elasticsearch & Kibana**: Elasticsearch consumes structured data from the `parsed.articles` topic, indexing it for full-text search. Kibana provides a powerful UI for developers to explore the indexed data, build visualizations, and debug search queries.
*   **ClickHouse Analytics**: The Parser Worker streams performance metrics to ClickHouse. The FastAPI backend exposes an analytics endpoint that executes fast SQL aggregation queries against ClickHouse to power high-density charts in the React UI.

## Section 9. Scheduling & Orchestration Layer

*   **Tech Stack**: Apache Airflow
*   **Purpose**: Manages automated, schedule-based crawling. Airflow DAGs read a list of target sites (e.g., from `crawl_targets.json`), and trigger jobs at regular intervals by making authenticated calls to the FastAPI `/jobs` endpoint.
*   **Principle**: Airflow **orchestrates** the pipeline by initiating workflows; it does not **execute** the core crawling or parsing logic itself. This separation keeps the system modular and scalable.

## Section 10. Observability, Logging & Monitoring Layer

*   **Metrics (Prometheus & Grafana)**: Prometheus scrapes metrics from the API, workers, and Kafka exporters (e.g., via Jolokia). Grafana provides dashboards to visualize key performance indicators like API latency, CPU/Memory usage per worker, and Kafka consumer group lag.
*   **Logging (Promtail, Loki & Grafana)**: Promtail is a log-shipping agent that runs alongside each worker. It collects logs, adds metadata (e.g., `worker_type="parser-worker"`), and forwards them to Loki. Loki is a horizontally-scalable, multi-tenant log aggregation system. Grafana is used to query Loki and display logs from all services in a centralized, searchable UI for real-time debugging.

## Section 11. End-to-End Execution Workflows

### A. The Write / Ingestion Flow

1.  **Initiation**: A user (via UI) or Airflow (via schedule) sends a `POST` request to the FastAPI `/jobs` endpoint.
2.  **Dispatch**: FastAPI validates the request, writes job metadata to **PostgreSQL**, and produces a `CrawlRequest` message to the `crawl.requests` Kafka topic.
3.  **Crawl**: The appropriate worker (`Surface`, `Deep`, or `Dark`) consumes the message, performs the web crawl, and produces the resulting `CrawlResult` (containing raw HTML) to the `raw.html.ingest` Kafka topic.
4.  **Parse**: A `Parser Worker` consumes the raw HTML, extracts Amharic text and metadata, and performs two actions in parallel:
    *   Produces a structured `ParsedItem` message to the `parsed.articles` Kafka topic.
    *   Streams performance metrics (latency, size) for this job to **ClickHouse**.
5.  **Indexing & Storage**:
    *   An **Elasticsearch** sink connector (or a dedicated consumer) reads from `parsed.articles` and indexes the document for search.
    *   The raw HTML from the `CrawlResult` is archived in **MinIO** for long-term storage and retrieval.

### B. The Read / Query Flow

1.  **Full-Text Search Flow**:
    *   A user types a query into the React UI.
    *   The UI sends the query to the FastAPI `/search` endpoint.
    *   FastAPI constructs a secure Elasticsearch query and returns the results to the UI.
2.  **Analytics & Dashboard Flow**:
    *   The user navigates to a dashboard page in the React UI.
    *   The UI requests analytics data from the FastAPI `/analytics` endpoint.
    *   FastAPI executes an aggregation query against **ClickHouse** and returns the summarized data for charting.
3.  **Raw File & Export Download Flow**:
    *   A user clicks a "Download HTML" or "Download CSV" button in the React UI.
    *   The UI requests a download URL from the FastAPI `/exports/{file_id}` endpoint.
    *   FastAPI generates a short-lived, pre-signed URL for the requested object in **MinIO** and returns it to the client.
    *   The user's browser downloads the file directly and securely from MinIO using the pre-signed URL.
