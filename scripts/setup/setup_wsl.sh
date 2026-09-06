#!/usr/bin/env bash
# =============================================================================
# setup_wsl.sh — Bootstrap WSL Ubuntu as a distributed worker node
#
# This script:
#   1. Validates WSL environment (Python 3.12+, git, pip)
#   2. Creates a native WSL project directory (avoid /mnt/c performance)
#   3. Sets up a Python virtualenv with all worker dependencies
#   4. Verifies connectivity to all Docker services on the Windows host
#   5. Optionally starts workers in the background
#
# Usage (from Windows Git Bash or WSL):
#   bash scripts/setup/setup_wsl.sh
#
# Or from inside WSL directly:
#   ./scripts/setup/setup_wsl.sh
# =============================================================================

set -euo pipefail

# --- Colors ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log()   { echo -e "${GREEN}[SETUP]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# =============================================================================
# Configuration
# =============================================================================

WSL_PROJECT_DIR="${WSL_PROJECT_DIR:-$HOME/duka-scraper}"
VENV_DIR="${WSL_PROJECT_DIR}/.venv"
# Windows project path (accessible via /mnt/c/)
WIN_PROJECT_PATH="/mnt/c/Users/mesfi/Projects/Duka_Scraper"

# Service ports to verify
declare -A SERVICES=(
    ["Kafka-EXTERNAL"]=29092
    ["PostgreSQL"]=5432
    ["Redis"]=6379
    ["MinIO-S3"]=9000
    ["Elasticsearch"]=9200
    ["ClickHouse"]=8123
    ["Qdrant"]=6333
    ["Ollama"]=11435
    ["Tor"]=9050
    ["API"]=8000
)

# =============================================================================
# Step 1: Validate Environment
# =============================================================================

log "Step 1: Validating WSL environment..."

# Check Python version
PYTHON_VERSION=$(python3 -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
PYTHON_MAJOR=$(echo "$PYTHON_VERSION" | cut -d. -f1)
PYTHON_MINOR=$(echo "$PYTHON_VERSION" | cut -d. -f2)

if [ "$PYTHON_MAJOR" -lt 3 ] || { [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 12 ]; }; then
    error "Python 3.12+ required, found ${PYTHON_VERSION}"
fi
log "  Python ${PYTHON_VERSION} ✓"

# Check git
if ! command -v git &>/dev/null; then
    error "git not found. Install with: sudo apt install git"
fi
log "  git $(git --version | awk '{print $3}') ✓"

# Check pip
if ! python3 -m pip --version &>/dev/null; then
    error "pip not found. Install with: sudo apt install python3-pip"
fi
log "  pip ✓"

# Check venv
if ! python3 -c "import venv" 2>/dev/null; then
    error "venv module not found. Install with: sudo apt install python3-venv"
fi
log "  venv ✓"

# =============================================================================
# Step 2: Set Up Project Directory
# =============================================================================

log "Step 2: Setting up project in native WSL filesystem..."

if [ -d "$WSL_PROJECT_DIR" ]; then
    warn "Project directory already exists at ${WSL_PROJECT_DIR}"
    read -rp "  Update existing? (y/N): " CONFIRM
    if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
        log "Skipping directory setup."
    else
        log "  Pulling latest changes..."
        cd "$WSL_PROJECT_DIR"
        git pull --quiet 2>/dev/null || warn "Could not pull (not a git repo?)"
    fi
else
    if [ -d "$WIN_PROJECT_PATH" ]; then
        log "  Copying project from Windows filesystem (this may take a moment)..."
        # Use rsync for efficient copy (preserves .git, skips __pycache__)
        rsync -a --exclude='__pycache__' --exclude='.venv' --exclude='node_modules' \
              --exclude='.pytest_cache' --exclude='.ruff_cache' \
              "$WIN_PROJECT_PATH/" "$WSL_PROJECT_DIR/"
        log "  Project copied to ${WSL_PROJECT_DIR} ✓"
    else
        log "  Windows project not found at ${WIN_PROJECT_PATH}"
        log "  Creating minimal project structure..."
        mkdir -p "$WSL_PROJECT_DIR"
        cd "$WSL_PROJECT_DIR"
        git init 2>/dev/null || true
        warn "  Please ensure the full project is available in ${WSL_PROJECT_DIR}"
    fi
fi

cd "$WSL_PROJECT_DIR"

# =============================================================================
# Step 3: Create Virtual Environment
# =============================================================================

log "Step 3: Setting up Python virtual environment..."

if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
    log "  Virtual environment created at ${VENV_DIR} ✓"
else
    log "  Virtual environment already exists ✓"
fi

# Activate
source "$VENV_DIR/bin/activate"

# Upgrade pip
pip install --quiet --upgrade pip

# Install combined requirements
if [ -f "workers/requirements-wsl.txt" ]; then
    log "  Installing WSL worker dependencies..."
    pip install --quiet -r workers/requirements-wsl.txt
    log "  Dependencies installed ✓"
elif [ -f "requirements.txt" ]; then
    log "  Installing root requirements..."
    pip install --quiet -r requirements.txt
    if [ -f "workers/requirements.txt" ]; then
        pip install --quiet -r workers/requirements.txt
    fi
    log "  Dependencies installed ✓"
else
    warn "  No requirements.txt found — install dependencies manually"
fi

# =============================================================================
# Step 4: Verify Service Connectivity
# =============================================================================

log "Step 4: Verifying connectivity to Docker services..."

CONNECTED=0
FAILED=0

for SERVICE_NAME in "${!SERVICES[@]}"; do
    PORT=${SERVICES[$SERVICE_NAME]}
    if python3 -c "
import socket
s = socket.socket()
s.settimeout(2)
s.connect(('localhost', ${PORT}))
s.close()
" 2>/dev/null; then
        log "  ✓ ${SERVICE_NAME} (port ${PORT})"
        ((CONNECTED++))
    else
        warn "  ✗ ${SERVICE_NAME} (port ${PORT}) — not reachable"
        ((FAILED++))
    fi
done

log "  Results: ${CONNECTED} connected, ${FAILED} failed"

if [ "$FAILED" -gt 0 ]; then
    warn "Some services are not reachable. Ensure Docker Desktop is running."
    warn "Run 'docker ps' from Windows to verify containers are up."
fi

# =============================================================================
# Step 5: Create Environment File
# =============================================================================

log "Step 5: Creating .env for WSL workers..."

ENV_FILE="${WSL_PROJECT_DIR}/.env.wsl"
if [ ! -f "$ENV_FILE" ]; then
    cat > "$ENV_FILE" << 'ENVEOF'
# =============================================================================
# WSL Worker Environment Variables
# Copy values from the Windows .env or set explicitly
# =============================================================================

# Must be 'wsl' for endpoint overrides
APP_ENV=wsl

# Kafka (EXTERNAL listener on Windows Docker host)
KAFKA_BOOTSTRAP_SERVERS=localhost:29092

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=duka_system

# Redis
REDIS_URL=redis://localhost:6379/0

# MinIO
MINIO_ENDPOINT=localhost:9000
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# ClickHouse
CLICKHOUSE_HOST=localhost
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_DB=duka_scraper

# Elasticsearch
ELASTICSEARCH_URL=http://localhost:9200

# LLM (same API key as Windows)
# HOSTED_LLM_API_KEY=your_key_here
# HOSTED_LLM_URL=https://api.groq.com/openai/v1/chat/completions
# HOSTED_LLM_MODEL=openai/gpt-oss-120b

# RAG / Embedding (Ollama on host port 11435)
RAG_ENABLED=true
EMBEDDING_PROVIDER=ollama
EMBEDDING_BASE_URL=http://localhost:11435
EMBEDDING_MODEL=nomic-embed-text
VECTOR_DB_URL=http://localhost:6333
VECTOR_DB_COLLECTION=duka_articles

# Tor
tor_proxy_url=socks5://localhost:9050
ENVEOF
    log "  Created ${ENV_FILE} ✓"
    warn "  EDIT THIS FILE: Set HOSTED_LLM_API_KEY and other secrets!"
else
    log "  .env.wsl already exists ✓"
fi

# =============================================================================
# Step 6: Create Convenience Script
# =============================================================================

log "Step 6: Creating worker launcher script..."

LAUNCHER="${WSL_PROJECT_DIR}/start_workers.sh"
cat > "$LAUNCHER" << 'LAUNCHEOF'
#!/usr/bin/env bash
# =============================================================================
# start_workers.sh — Launch WSL workers for distributed processing
#
# Usage:
#   ./start_workers.sh                    # Start all recommended workers
#   ./start_workers.sh surface parser     # Start specific workers only
#   ./start_workers.sh --status           # Show running worker status
#   ./start_workers.sh --stop             # Stop all WSL workers
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv"
LOG_DIR="${SCRIPT_DIR}/logs/wsl-workers"
PID_DIR="${SCRIPT_DIR}/.pids"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Worker definitions: name -> module path
declare -A WORKER_MODULES=(
    ["surface"]="workers.surface-worker.main"
    ["parser"]="workers.parser-worker.main"
    ["llm"]="workers.llm-worker.main"
    ["exporter"]="workers.exporter-worker.main"
    ["deep"]="workers.deep-worker.main"
    ["dark"]="workers.dark-worker.main"
)

# Default workers (recommended for WSL distribution)
DEFAULT_WORKERS="surface parser llm exporter"

mkdir -p "$LOG_DIR" "$PID_DIR"

# Activate virtualenv
if [ -f "$VENV_DIR/bin/activate" ]; then
    source "$VENV_DIR/bin/activate"
else
    echo -e "${RED}[ERROR]${NC} Virtualenv not found at ${VENV_DIR}"
    echo "Run setup_wsl.sh first."
    exit 1
fi

# Load environment
if [ -f "${SCRIPT_DIR}/.env.wsl" ]; then
    set -a
    source "${SCRIPT_DIR}/.env.wsl"
    set +a
fi

export APP_ENV=wsl
export PYTHONPATH="${SCRIPT_DIR}"

# --- Functions ---

show_status() {
    echo -e "${GREEN}WSL Worker Status${NC}"
    echo "=================="
    for NAME in "${!WORKER_MODULES[@]}"; do
        PID_FILE="${PID_DIR}/${NAME}.pid"
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if kill -0 "$PID" 2>/dev/null; then
                echo -e "  ${GREEN}RUNNING${NC}  ${NAME} (PID ${PID})"
            else
                echo -e "  ${RED}STOPPED${NC}  ${NAME} (stale PID file)"
                rm -f "$PID_FILE"
            fi
        else
            echo -e "  ${YELLOW}IDLE${NC}     ${NAME}"
        fi
    done
    echo ""
    echo "Kafka consumer groups (querying...):"
    docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server localhost:9092 --list 2>/dev/null | grep -E "surface|parser|llm|exporter" || echo "  (no active WSL consumer groups found)"
}

stop_workers() {
    echo -e "${YELLOW}Stopping WSL workers...${NC}"
    for NAME in "${!WORKER_MODULES[@]}"; do
        PID_FILE="${PID_DIR}/${NAME}.pid"
        if [ -f "$PID_FILE" ]; then
            PID=$(cat "$PID_FILE")
            if kill -0 "$PID" 2>/dev/null; then
                echo "  Stopping ${NAME} (PID ${PID})..."
                kill "$PID" 2>/dev/null || true
                # Wait up to 10s for graceful shutdown
                for i in $(seq 1 10); do
                    kill -0 "$PID" 2>/dev/null || break
                    sleep 1
                done
                # Force kill if still running
                kill -9 "$PID" 2>/dev/null || true
            fi
            rm -f "$PID_FILE"
        fi
    done
    echo -e "${GREEN}All WSL workers stopped.${NC}"
}

start_worker() {
    local NAME=$1
    local MODULE=${WORKER_MODULES[$NAME]}

    if [ -z "$MODULE" ]; then
        echo -e "${RED}[ERROR]${NC} Unknown worker: ${NAME}"
        echo "Available: ${!WORKER_MODULES[*]}"
        return 1
    fi

    PID_FILE="${PID_DIR}/${NAME}.pid"
    LOG_FILE="${LOG_DIR}/${NAME}.log"

    # Check if already running
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo -e "  ${YELLOW}SKIP${NC}  ${NAME} already running (PID ${PID})"
            return 0
        fi
        rm -f "$PID_FILE"
    fi

    echo -e "  ${GREEN}START${NC} ${NAME}..."
    nohup python -m "$MODULE" >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "         PID: $! | Log: ${LOG_FILE}"
}

# --- Main ---

case "${1:-}" in
    --status|-s)
        show_status
        exit 0
        ;;
    --stop)
        stop_workers
        exit 0
        ;;
    --help|-h)
        echo "Usage: $0 [WORKER_NAMES...] [--status|--stop]"
        echo ""
        echo "Workers: ${!WORKER_MODULES[*]}"
        echo "Default: ${DEFAULT_WORKERS}"
        exit 0
        ;;
    "")
        WORKERS_TO_START=$DEFAULT_WORKERS
        ;;
    *)
        WORKERS_TO_START="$*"
        ;;
esac

echo -e "${GREEN}Starting WSL workers...${NC}"
echo "APP_ENV=${APP_ENV}"
echo "Kafka: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:29092}"
echo "Log dir: ${LOG_DIR}"
echo ""

for WORKER in $WORKERS_TO_START; do
    start_worker "$WORKER"
done

echo ""
echo -e "${GREEN}Workers launched.${NC} Use '$0 --status' to check, '$0 --stop' to stop."
LAUNCHEOF

chmod +x "$LAUNCHER"
log "  Created ${LAUNCHER} ✓"

# =============================================================================
# Done
# =============================================================================

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  WSL Worker Distribution Setup Complete${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "  Project:       ${WSL_PROJECT_DIR}"
echo "  Virtualenv:    ${VENV_DIR}"
echo "  Environment:   ${ENV_FILE}"
echo "  Launcher:      ${LAUNCHER}"
echo ""
echo "  Next steps:"
echo "    1. Edit .env.wsl — set HOSTED_LLM_API_KEY and other secrets"
echo "    2. cd ${WSL_PROJECT_DIR}"
echo "    3. ./start_workers.sh --status    # Check service connectivity"
echo "    4. ./start_workers.sh             # Start all recommended workers"
echo ""
echo "  Workers will join the SAME Kafka consumer groups as Docker workers,"
echo "  and Kafka will automatically distribute partitions between them."
echo ""
