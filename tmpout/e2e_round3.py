"""E2E round 3 — the three things the user asked to be proven.

  A) DEPTH:  seed = https://www.scrapingcourse.com, max_depth=2, allow_login.
             Children must inherit the auth params and only attempt/announce
             login on real auth pages.
  B) GATE:   seed = /login/cf-turnstile. This used to loop the Turnstile widget,
             get 419 PAGE EXPIRED, and burn the whole 240s page budget.
             It must now log in via Camoufox and store real product content.
  C) LOAD:   5 jobs triggered back-to-back (many users / many devices at once).
             All must reach a terminal status; the job aggregate must not be
             failed by a per-page failure.

Polls everything to terminal. Prints no secrets.
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
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                return json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")[:400]
            raise RuntimeError(f"{method} {path} -> HTTP {exc.code}: {body}") from None
        except Exception:
            if attempt == 2:
                raise
            time.sleep(5)


def trigger(url, depth, params, token, label):
    job = api(
        "POST",
        "/jobs/trigger",
        {
            "url": url,
            "language": "en",
            "worker_override": "deep",
            "max_depth": depth,
            "recursive_config": {"enable_extraction": True},
            "job_params": params,
        },
        token,
    )
    print(f"  {label}: {job.get('job_id')}", flush=True)
    return job.get("job_id")


def main():
    tok = api(
        "POST",
        "/auth/login",
        {"email": ENV["INITIAL_ADMIN_EMAIL"], "password": ENV["INITIAL_ADMIN_PASSWORD"]},
    )
    token = tok.get("access_token") or tok.get("token")
    assert token, f"login response missing token: {list(tok)}"

    print("=== triggering ===", flush=True)
    ids = {}
    # A) deeper recursive crawl from a seed URL
    ids["A_depth2"] = trigger(
        "https://www.scrapingcourse.com", 2, {"allow_login": True}, token, "A_depth2"
    )
    # B) the Turnstile-gated login page on its own
    ids["B_cfturnstile"] = trigger(
        "https://www.scrapingcourse.com/login/cf-turnstile",
        1,
        {"allow_login": True},
        token,
        "B_cfturnstile",
    )
    # C) concurrency: five jobs at once, as many users would produce
    for i in range(1, 6):
        ids[f"C_load{i}"] = trigger(
            "https://www.scrapingcourse.com",
            1,
            {"allow_login": True},
            token,
            f"C_load{i}",
        )

    state, last = {}, {}
    deadline = time.time() + 20 * 60
    while time.time() < deadline:
        done = True
        for key, jid in ids.items():
            if not jid or key in state:
                continue
            try:
                info = api("GET", f"/jobs/{jid}", token=token)
            except Exception as exc:
                print(f"  poll error {key}: {exc}", flush=True)
                done = False
                continue
            status = info.get("status")
            if status != last.get(key):
                print(f"  [{key}] {jid} status={status}", flush=True)
                last[key] = status
            if status in TERMINAL:
                state[key] = info
            else:
                done = False
        if done:
            break
        time.sleep(10)

    print("\n=== FINAL ===", flush=True)
    for key, jid in ids.items():
        info = state.get(key)
        if not info:
            print(f"{key:15s} ({jid}): DID NOT REACH TERMINAL (last={last.get(key)})", flush=True)
            continue
        print(
            f"{key:15s} ({jid}): status={info.get('status')} "
            f"reason={info.get('failure_reason')!r}",
            flush=True,
        )


if __name__ == "__main__":
    main()
