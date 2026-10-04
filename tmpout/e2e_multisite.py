"""Multi-site concurrency: five DISTINCT sites' pages at once.

The trigger endpoint has an intentional duplicate-job guard (same URL + same
user returns the running job), so a load test has to submit different URLs or
it just measures the guard. These are five different pages of one site, which
is what a handful of users submitting at the same moment looks like.

Polled in Postgres, not over HTTP: the host's client to the API flakes with
WinError 10054 under exactly this kind of load.
"""
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

BASE = "http://localhost:8000/api/v1"
TERMINAL = {"completed", "failed", "skipped", "needs_review"}

URLS = [
    "https://www.scrapingcourse.com/ecommerce",
    "https://www.scrapingcourse.com/table-parsing",
    "https://www.scrapingcourse.com/login/csrf",
    "https://www.scrapingcourse.com/cloudflare-challenge",
    "https://www.scrapingcourse.com/login/cf-antibot",
]


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
    for _ in range(4):
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


def status_of(job_id):
    rows = psql(f"SELECT status FROM jobs WHERE job_id='{job_id}';")
    return rows[0].split("|")[0] if rows else None


def main():
    env = load_env()
    tok = api("POST", "/auth/login", {
        "email": env["INITIAL_ADMIN_EMAIL"],
        "password": env["INITIAL_ADMIN_PASSWORD"],
    })
    token = tok.get("access_token") or tok.get("token")
    assert token

    ids = []
    for url in URLS:
        job = api("POST", "/jobs/trigger", {
            "url": url, "language": "en", "worker_override": "deep",
            "max_depth": 1, "recursive_config": {"enable_extraction": True},
            "job_params": {"allow_login": True},
        }, token)
        ids.append((url, job.get("job_id")))
        print(f"  {job.get('job_id')}  {url}", flush=True)

    distinct = len({j for _, j in ids})
    print(f"\nsubmitted {len(ids)} jobs, {distinct} distinct ids", flush=True)

    deadline = time.time() + 18 * 60
    done = {}
    while time.time() < deadline and len(done) < len(ids):
        for url, jid in ids:
            if jid in done:
                continue
            st = status_of(jid)
            if st in TERMINAL:
                done[jid] = (url, st)
                print(f"  {jid} -> {st}  ({url})", flush=True)
        time.sleep(10)

    print("\n=== FINAL ===", flush=True)
    for url, jid in ids:
        st = status_of(jid)
        log = psql(
            "SELECT status, count(*) FROM crawl_log WHERE job_id="
            f"'{jid}' GROUP BY 1 ORDER BY 2 DESC;"
        )
        summary = ", ".join(
            f"{r.split('|')[1]}{r.split('|')[0]}" for r in log
        ) or "-"
        flag = "" if jid in done else "  (NOT TERMINAL)"
        print(f"  {jid} {st:<12} pages[{summary}]  {url}{flag}", flush=True)

    ok = sum(1 for jid in done if done[jid][1] == "completed")
    print(f"\n{ok}/{len(ids)} completed", flush=True)


if __name__ == "__main__":
    main()