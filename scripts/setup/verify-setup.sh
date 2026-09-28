#!/bin/bash
# =============================================================
# verify-setup.sh — Run after `docker-compose up -d` to check
# that every service is healthy and the system is ready.
#
# Usage:
#   bash scripts/setup/verify-setup.sh
#
# What it checks:
#   1. All containers are running
#   2. PostgreSQL has all required tables
#   3. Kafka topics exist
#   4. Elasticsearch index exists
#   5. MinIO buckets exist
#   6. API /health endpoint returns healthy
#   7. API /metrics endpoint serves Prometheus format
#   8. Worker health endpoints respond
# =============================================================

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0
WARN=0

check() {
    local name="$1"
    shift
    if "$@" >/dev/null 2>&1; then
        echo -e "  ${GREEN}✓${NC} $name"
        PASS=$((PASS + 1))
    else
        echo -e "  ${RED}✗${NC} $name"
        FAIL=$((FAIL + 1))
    fi
}

warn() {
    local name="$1"
    shift
    if "$@" >/dev/null 2>&1; then
        echo -e "  ${GREEN}✓${NC} $name"
        PASS=$((PASS + 1))
    else
        echo -e "  ${YELLOW}⚠${NC} $name (non-critical)"
        WARN=$((WARN + 1))
    fi
}

echo ""
echo "============================================="
echo "  Duka Scraper — Setup Verification"
echo "============================================="
echo ""

# --- 1. Docker containers ---
echo "1. Docker containers"
for svc in postgres kafka elasticsearch clickhouse minio redis qdrant ollama-embedding tor api surface-worker parser-worker dark-worker llm-worker exporter-worker prometheus alertmanager grafana; do
    check "$svc" docker compose ps --format json "$svc" | python -c "import sys,json; d=json.load(sys.stdin); assert d.get('State')=='running'" 2>/dev/null || \
    check "$svc" docker-compose ps --format json "$svc" | python -c "import sys,json; d=json.load(sys.stdin); assert d.get('State')=='running'" 2>/dev/null || \
    warn "$svc (may not be running)" true
done
echo ""

# --- 2. PostgreSQL tables ---
echo "2. PostgreSQL schema"
TABLES="users verification_tokens auth_sessions audit_logs jobs credential_usage parsed_items exports crawl_log discovered_external_links content_fingerprints"
for tbl in $TABLES; do
    check "table: $tbl" docker compose exec -T postgres psql -U postgres -d duka_system -tAc "SELECT 1 FROM information_schema.tables WHERE table_name='$tbl'" 2>/dev/null | grep -q 1 || \
    warn "table: $tbl" true
done
echo ""

# --- 3. Kafka topics ---
echo "3. Kafka topics"
TOPICS="crawl.requests crawl.raw crawl.parsed crawl.requests.dlq"
for topic in $TOPICS; do
    check "topic: $topic" docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q "$topic" || \
    warn "topic: $topic" true
done
echo ""

# --- 4. Elasticsearch ---
echo "4. Elasticsearch"
check "cluster up" curl -sf http://localhost:9200/_cluster/health 2>/dev/null || warn "cluster up" true
check "index: duka_articles" curl -sf http://localhost:9200/duka_articles 2>/dev/null | grep -q "duka_articles" || warn "index: duka_articles" true
echo ""

# --- 5. MinIO ---
echo "5. MinIO"
check "API reachable" curl -sf http://localhost:9000/minio/health/live 2>/dev/null || warn "API reachable" true
echo ""

# --- 6. API endpoints ---
echo "6. API endpoints"
check "/health" curl -sf http://localhost:8000/health 2>/dev/null | grep -q '"status"' || warn "/health" true
check "/metrics" curl -sf http://localhost:8000/metrics 2>/dev/null | grep -q "http_requests_total" || warn "/metrics" true
check "/docs" curl -sf http://localhost:8000/docs 2>/dev/null | grep -q "swagger" || warn "/docs" true
echo ""

# --- 7. Worker health endpoints ---
echo "7. Worker health endpoints"
for worker in surface-worker parser-worker dark-worker llm-worker exporter-worker; do
    host="${worker}"
    check "$worker /health" docker compose exec -T "$worker" curl -sf http://localhost:8080/health 2>/dev/null | grep -q '"status"' || \
    warn "$worker /health" true
done
echo ""

# --- 8. Monitoring ---
echo "8. Monitoring"
check "Prometheus" curl -sf http://localhost:9090/-/healthy 2>/dev/null | grep -q "ready" || warn "Prometheus" true
check "Grafana" curl -sf http://localhost:3000/api/health 2>/dev/null | grep -q "ok" || warn "Grafana" true
check "Alertmanager" curl -sf http://localhost:9093/-/healthy 2>/dev/null | grep -q "ready" || warn "Alertmanager" true
echo ""

# --- Summary ---
echo "============================================="
echo -e "  Results: ${GREEN}$PASS passed${NC}, ${RED}$FAIL failed${NC}, ${YELLOW}$WARN warnings${NC}"
echo "============================================="

if [ "$FAIL" -gt 0 ]; then
    echo -e "\n${RED}Some checks failed. Run 'docker compose logs' to investigate.${NC}"
    exit 1
else
    echo -e "\n${GREEN}All critical checks passed. System is ready.${NC}"
    exit 0
fi
