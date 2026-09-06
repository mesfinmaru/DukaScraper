#!/usr/bin/env bash
# =============================================================================
# start_wsl_workers.sh - Launch WSL distributed workers
#
# Usage (from inside WSL):
#   ./start_wsl_workers.sh                    # Start all recommended workers
#   ./start_wsl_workers.sh surface parser     # Start specific workers only
#   ./start_wsl_workers.sh --status           # Show running worker status
#   ./start_wsl_workers.sh --stop             # Stop all WSL workers
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv"
LOG_DIR="${SCRIPT_DIR}/logs/wsl-workers"
PID_DIR="${SCRIPT_DIR}/.pids"

GREEN="\033[0;32m"; YELLOW="\033[1;33m"; RED="\033[0;31m"; NC="\033[0m"

# Worker definitions: name -> module path
declare -A WORKER_MODULES=(
    ["surface"]="workers.surface-worker.main"
    ["parser"]="workers.parser-worker.main"
    ["llm"]="workers.llm-worker.main"
    ["exporter"]="workers.exporter-worker.main"
    ["deep"]="workers.deep-worker.main"
    ["dark"]="workers.dark-worker.main"
)

DEFAULT_WORKERS="surface parser llm exporter"
mkdir -p "$LOG_DIR" "$PID_DIR"

# Activate virtualenv
source "$VENV_DIR/bin/activate"

# Load WSL environment
set -a
source "${SCRIPT_DIR}/workers/.env.wsl"
set +a

# Ensure PYTHONPATH includes project root
export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH:-}"

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
                echo -e "  ${RED}STOPPED${NC}  ${NAME} (stale PID)"
                rm -f "$PID_FILE"
            fi
        else
            echo -e "  ${YELLOW}IDLE${NC}     ${NAME}"
        fi
    done
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
                for i in $(seq 1 10); do kill -0 "$PID" 2>/dev/null || break; sleep 1; done
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
    [ -z "$MODULE" ] && { echo -e "${RED}Unknown worker: ${NAME}${NC}"; return 1; }
    local PID_FILE="${PID_DIR}/${NAME}.pid"
    local LOG_FILE="${LOG_DIR}/${NAME}.log"

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
    --status|-s) show_status; exit 0 ;;
    --stop) stop_workers; exit 0 ;;
    --help|-h)
        echo "Usage: $0 [WORKER_NAMES...] [--status|--stop]"
        echo "Workers: ${!WORKER_MODULES[*]}"
        echo "Default: ${DEFAULT_WORKERS}"
        exit 0
        ;;
    "") WORKERS_TO_START=$DEFAULT_WORKERS ;;
    *) WORKERS_TO_START="$*" ;;
esac

echo -e "${GREEN}Starting WSL workers...${NC}"
echo "APP_ENV=${APP_ENV:-not set}"
echo "Kafka: ${KAFKA_BOOTSTRAP_SERVERS:-not set}"
echo "Log dir: ${LOG_DIR}"
echo ""

for WORKER in $WORKERS_TO_START; do
    start_worker "$WORKER"
done

echo ""
echo -e "${GREEN}Workers launched.${NC} Use '$0 --status' to check, '$0 --stop' to stop."
