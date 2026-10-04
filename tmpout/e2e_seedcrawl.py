"""E2E verification: seed URL crawl at increased depth + a concurrent 2nd job.

Logs in as admin, submits:
  A) https://www.scrapingcourse.com  depth=2, allow_login=true  (the failing scenario)
  B) https://books.toscrape.com      depth=1                    (concurrent second job)
both forced to the deep worker, then polls both jobs to a terminal status.
Prints no secrets.
"""
import json
import time
import urllib.error
import urllib.request

BASE = "http://localhost:8000/api/v1"
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


ENV = load_env()


def api(method, path, data=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(
        BASE + path,
        method=method,
        data=json.dumps(data).encode() if data is not None else None,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")[:400]
        raise RuntimeError(f"{method} {path} -> HTTP {exc.code}: {body}") from None


def main():
    tok = api(
        "POST",
        "/auth/login",
        {"email": ENV["INITIAL_ADMIN_EMAIL"], "password": ENV["INITIAL_ADMIN_PASSWORD"]},
    )
    token = tok.get("access_token") or tok.get("token")
    assert token, f"login response missing token: {list(tok)}"

    job_a = api(
        "POST",
        "/jobs/trigger",
        {
            "url": "https://www.scrapingcourse.com",
            "language": "en",
            "worker_override": "deep",
            "max_depth": 2,
            "recursive_config": {"enable_extraction": True},
            "job_params": {"allow_login": True},
        },
        token,
    )
    print(f"job A: {json.dumps(job_a)}", flush=True)

    job_b = api(
        "POST",
        "/jobs/trigger",
        {
            "url": "https://books.toscrape.com",
            "language": "en",
            "worker_override": "deep",
            "max_depth": 1,
            "recursive_config": {"enable_extraction": True},
            "job_params": {},
        },
        token,
    )
    print(f"job B: {json.dumps(job_b)}", flush=True)

    ids = {
        "A": job_a.get("job_id"),
        "B": job_b.get("job_id"),
    }
    state = {}
    deadline = time.time() + 20 * 60
    while time.time() < deadline:
        done = True
        for key, jid in ids.items():
            if not jid or jid in state:
                continue
            try:
                info = api("GET", f"/jobs/{jid}", token=token)
            except Exception as exc:
                print(f"  poll error {key}: {exc}", flush=True)
                done = False
                continue
            status = info.get("status")
            if status != state.get(f"{key}_last"):
                print(f"  [{key}] {jid} status={status}", flush=True)
                state[f"{key}_last"] = status
            if status in TERMINAL:
                state[key] = info
            else:
                done = False
        if done:
            break
        time.sleep(10)

    print("\n=== FINAL ===", flush=True)
    for key in ("A", "B"):
        info = state.get(key)
        if not info:
            print(f"job {key} ({ids[key]}): did not reach terminal status", flush=True)
            continue
        print(
            f"job {key} ({ids[key]}): status={info.get('status')} "
            f"reason={info.get('failure_reason')!r} url={info.get('url')}",
            flush=True,
        )


if __name__ == "__main__":
    main()
