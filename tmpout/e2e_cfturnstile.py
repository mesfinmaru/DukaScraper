"""Focused E2E: the /login/cf-turnstile page, uncontended.

The host's HTTP client to the API flakes (WinError 10054) under load, so the
job is triggered over HTTP once and then polled in Postgres, which is the
ground truth anyway.

Prints no secrets.
"""
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE = "http://localhost:8000/api/v1"
TARGET = sys.argv[1] if len(sys.argv) > 1 else "https://www.scrapingcourse.com/login/cf-turnstile"
DEPTH = int(sys.argv[2]) if len(sys.argv) > 2 else 1
TERMINAL = {"completed", "failed", "skipped", "needs_review"}


def load_env(path=".env"):
    out = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def api(method, path, data=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(
        BASE + path, method=method,
        data=json.dumps(data).encode() if data is not None else None,
        headers=headers,
    )
    last = None
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                return json.loads(resp.read() or b"{}")
        except Exception as exc:
            last = exc
            time.sleep(4)
    raise RuntimeError(f"{method} {path}: {last}")


def psql(sql):
    out = subprocess.run(
        ["docker", "exec", "postgres", "psql", "-U", "postgres", "-d", "duka_system",
         "-t", "-A", "-F", "|", "-c", sql],
        capture_output=True, text=True,
    )
    return [ln for ln in out.stdout.splitlines() if ln.strip()]


def job_status(job_id):
    rows = psql(f"SELECT status, active_tasks FROM jobs WHERE job_id='{job_id}';")
    return rows[0].split("|") if rows else [None, None]


def crawl_log(job_id):
    rows = psql(
        "SELECT status, count(*) FROM crawl_log WHERE job_id="
        f"'{job_id}' GROUP BY 1 ORDER BY 2 DESC;"
    )
    return ", ".join(f"{r.split('|')[1]} {r.split('|')[0]}" for r in rows) or "(none)"


def main():
    env = load_env()
    tok = api("POST", "/auth/login", {
        "email": env["INITIAL_ADMIN_EMAIL"],
        "password": env["INITIAL_ADMIN_PASSWORD"],
    })
    token = tok.get("access_token") or tok.get("token")
    assert token

    job = api("POST", "/jobs/trigger", {
        "url": TARGET, "language": "en", "worker_override": "deep",
        "max_depth": DEPTH, "recursive_config": {"enable_extraction": True},
        "job_params": {"allow_login": True},
    }, token)
    jid = job.get("job_id")
    print(f"job {jid}  url={TARGET}  depth={DEPTH}", flush=True)

    deadline = time.time() + 12 * 60
    last = None
    while time.time() < deadline:
        status, active = (job_status(jid) + [None, None])[:2]
        if status != last:
            print(f"  [{int(time.time() - (deadline - 720))}] status={status} "
                  f"active={active} log=[{crawl_log(jid)}]", flush=True)
            last = status
        if status in TERMINAL:
            break
        time.sleep(10)

    print(f"\nFINAL {jid}: status={job_status(jid)[0]}", flush=True)
    print(f"crawl_log: {crawl_log(jid)}", flush=True)
    rows = psql(
        "SELECT url, status FROM crawl_log WHERE job_id="
        f"'{jid}' ORDER BY created_at;"
    )
    for r in rows:
        print("  ", r.replace("|", " -> "), flush=True)


if __name__ == "__main__":
    main()