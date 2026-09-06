"""
distributed_cluster_status.py - Distributed Cluster Health Monitor

Shows the status of the entire distributed worker cluster:
  - Kafka consumer group lag per group
  - Which workers are active (Docker vs WSL vs other nodes)
  - Service health checks
  - Partition assignment distribution

Usage:
    python scripts/monitoring/distributed_cluster_status.py
    python scripts/monitoring/distributed_cluster_status.py --json
    python scripts/monitoring/distributed_cluster_status.py --watch
"""

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime

# --- Configuration ---

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
CONSUMER_GROUPS = [
    "surface-group",
    "parser-group",
    "llm-worker-group",
    "exporter-group",
    "deep-group",
    "dark-group",
]

SERVICES = {
    "Kafka (EXTERNAL)": 29092,
    "Kafka (PLAINTEXT)": 9092,
    "PostgreSQL": 5432,
    "Redis": 6379,
    "MinIO S3": 9000,
    "Elasticsearch": 9200,
    "ClickHouse": 8123,
    "Qdrant": 6333,
    "Ollama": 11435,
    "Tor": 9050,
    "API": 8000,
}

DOCKER_CONTAINER_PREFIX = "duka_scraper"

SEP = "=" * 72
DIV = "-" * 72


def check_port(host: str, port: int, timeout: float = 2.0) -> bool:
    """Check if a TCP port is reachable."""
    try:
        s = socket.socket()
        s.settimeout(timeout)
        s.connect((host, port))
        s.close()
        return True
    except (ConnectionRefusedError, TimeoutError, OSError):
        return False


def get_kafka_consumer_groups() -> dict:
    """Query Kafka for consumer group details."""
    result = {}
    try:
        # List active consumer groups
        cmd = [
            "docker", "exec", "kafka",
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server", "kafka:9092",
            "--list",
        ]
        output = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        active_groups = [g.strip() for g in output.stdout.strip().split("\n") if g.strip()]

        for group in CONSUMER_GROUPS:
            if group not in active_groups:
                result[group] = {"status": "inactive", "members": 0, "lag": "N/A"}
                continue

            # Get group details
            detail_cmd = [
                "docker", "exec", "kafka",
                "/opt/kafka/bin/kafka-consumer-groups.sh",
                "--bootstrap-server", "kafka:9092",
                "--group", group,
                "--describe",
            ]
            detail_output = subprocess.run(detail_cmd, capture_output=True, text=True, timeout=10)

            members = set()
            total_lag = 0
            partitions = []

            for line in detail_output.stdout.strip().split("\n"):
                if line.startswith("GROUP") or not line.strip():
                    continue
                parts = line.split()
                if len(parts) >= 6:
                    member_id = parts[4] if len(parts) > 4 else ""
                    if member_id and member_id != "-" and member_id != "owner":
                        members.add(member_id.split("-")[0][:20])  # Shorten for display

                    try:
                        lag = int(parts[5]) if parts[5] not in ("-", "") else 0
                        total_lag += lag
                    except (ValueError, IndexError):
                        pass

                    partitions.append({
                        "partition": parts[1] if len(parts) > 1 else "?",
                        "offset": parts[2] if len(parts) > 2 else "?",
                        "end_offset": parts[3] if len(parts) > 3 else "?",
                        "lag": parts[5] if len(parts) > 5 else "?",
                    })

            result[group] = {
                "status": "active",
                "members": len(members),
                "member_ids": list(members),
                "total_lag": total_lag,
                "partitions": partitions,
            }
    except Exception as e:
        result["error"] = str(e)

    return result


def get_docker_workers() -> list:
    """List running Docker worker containers."""
    try:
        cmd = [
            "docker", "ps", "--format",
            "{{.Names}}\t{{.Status}}\t{{.Ports}}",
            "--filter", "name=worker",
        ]
        output = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        workers = []
        for line in output.stdout.strip().split("\n"):
            if line.strip():
                parts = line.split("\t")
                workers.append({
                    "name": parts[0] if len(parts) > 0 else "?",
                    "status": parts[1] if len(parts) > 1 else "?",
                })
        return workers
    except Exception:
        return []


def get_wsl_workers() -> list:
    """Check for WSL worker processes (looks for python processes running worker modules)."""
    workers = []
    try:
        # Check if WSL is available
        wsl_check = subprocess.run(
            ["wsl", "-d", "Ubuntu", "--", "pgrep", "-f", "workers.*main.py"],
            capture_output=True, text=True, timeout=5,
        )
        if wsl_check.returncode == 0 and wsl_check.stdout.strip():
            pids = wsl_check.stdout.strip().split("\n")
            for pid in pids:
                pid = pid.strip()
                if not pid:
                    continue
                # Get the command line
                cmdline_cmd = subprocess.run(
                    ["wsl", "-d", "Ubuntu", "--", "cat", f"/proc/{pid}/cmdline"],
                    capture_output=True, text=True, timeout=5,
                )
                if cmdline_cmd.returncode == 0:
                    cmd = cmdline_cmd.stdout.replace("\x00", " ").strip()
                    # Extract worker name from path
                    for wk in ["surface", "parser", "llm", "exporter", "deep", "dark"]:
                        if wk in cmd:
                            workers.append({"name": f"wsL-{wk}", "status": f"PID {pid}", "cmd": cmd[:80]})
                            break
    except Exception:
        pass
    return workers


def print_report(services: dict, kafka_groups: dict, docker_workers: list, wsl_workers: list):
    """Print a formatted cluster status report."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n{SEP}")
    print(f"  DUKA DISTRIBUTED CLUSTER STATUS  --  {now}")
    print(SEP)

    # --- Service Health ---
    print(f"\n{DIV}")
    print("  SERVICE HEALTH")
    print(DIV)
    for name, port in SERVICES.items():
        ok = check_port("localhost", port)
        indicator = "[OK]" if ok else "[--]"
        print(f"  {indicator}  {name:<25} port {port}")

    # --- Docker Workers ---
    print(f"\n{DIV}")
    print("  DOCKER WORKERS (Windows Host)")
    print(DIV)
    if docker_workers:
        for w in docker_workers:
            print(f"  {w['name']:<25} {w['status']}")
    else:
        print("  (no Docker worker containers found)")

    # --- WSL Workers ---
    print(f"\n{DIV}")
    print("  WSL WORKERS (Ubuntu WSL -- Distributed Node)")
    print(DIV)
    if wsl_workers:
        for w in wsl_workers:
            print(f"  {w['name']:<25} {w['status']}")
            if "cmd" in w:
                print(f"  {'':25} {w['cmd']}")
    else:
        print("  (no WSL worker processes found)")
        print("  Tip: Run ./start_workers.sh in WSL to launch distributed workers")

    # --- Kafka Consumer Groups ---
    print(f"\n{DIV}")
    print("  KAFKA CONSUMER GROUPS (Cross-Node Distribution)")
    print(DIV)
    if "error" in kafka_groups:
        print(f"  Error: {kafka_groups['error']}")
    else:
        for group, info in kafka_groups.items():
            status = info.get("status", "?")
            members = info.get("members", 0)
            lag = info.get("total_lag", "N/A")

            if status == "active":
                indicator = "[OK]"
                lag_str = f"lag={lag}" if isinstance(lag, int) else f"lag={lag}"
                print(f"  {indicator}  {group:<25} members={members}  {lag_str}")
                # Show member IDs for cross-node identification
                member_ids = info.get("member_ids", [])
                if member_ids:
                    for mid in member_ids:
                        print(f"        -> {mid}")
            else:
                print(f"  [--]  {group:<25} (inactive)")

    print(f"\n{SEP}")
    print("  TIP: Workers across Docker and WSL share Kafka consumer groups.")
    print("  Kafka automatically distributes partitions when workers join/leave.")
    print(f"{SEP}\n")


def main():
    parser = argparse.ArgumentParser(description="Distributed cluster status monitor")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--watch", action="store_true", help="Refresh every 10 seconds")
    args = parser.parse_args()

    while True:
        services_status = {}
        for name, port in SERVICES.items():
            services_status[name] = check_port("localhost", port)

        kafka_groups = get_kafka_consumer_groups()
        docker_workers = get_docker_workers()
        wsl_workers = get_wsl_workers()

        if args.json:
            report = {
                "timestamp": datetime.now().isoformat(),
                "services": services_status,
                "kafka_groups": kafka_groups,
                "docker_workers": docker_workers,
                "wsl_workers": wsl_workers,
            }
            print(json.dumps(report, indent=2, default=str))
        else:
            print_report(services_status, kafka_groups, docker_workers, wsl_workers)

        if not args.watch:
            break

        try:
            time.sleep(10)
        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()
