#!/bin/bash
# Wait for Kafka to be reachable before starting the worker process.
# Usage: /wait_for_kafka.sh <command> [args...]

echo "Waiting for Kafka to be ready..."

for i in {1..30}; do
  if python3 -c "import socket; socket.create_connection(('kafka', 9092), timeout=2)" 2>/dev/null; then
    echo "Kafka is ready!"
    exec "$@"
  fi
  echo "Attempt $i/30: Kafka not ready yet, waiting..."
  sleep 2
done

echo "Timeout: Kafka failed to start"
exit 1
