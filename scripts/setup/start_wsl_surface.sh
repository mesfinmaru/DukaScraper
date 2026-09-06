#!/usr/bin/env bash
# Start surface worker in WSL as a detached background process
cd ~/duka-scraper
source .venv/bin/activate
export APP_ENV=wsl
export PYTHONPATH=.
export KAFKA_BOOTSTRAP_SERVERS=localhost:29092

mkdir -p .pids logs/wsl-workers

nohup python -m workers.surface-worker.main \
    >> logs/wsl-workers/surface.log 2>&1 &

echo $! > .pids/surface.pid
echo "Surface worker started with PID $(cat .pids/surface.pid)"
