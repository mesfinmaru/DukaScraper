import json, math, httpx
BASE = "http://127.0.0.1:8000"

def walk(o, path="$"):
    if isinstance(o, dict):
        for k, v in o.items():
            yield from walk(v, f"{path}.{k}")
    elif isinstance(o, list):
        for i, v in enumerate(o):
            yield from walk(v, f"{path}[{i}]")
    elif isinstance(o, float) and not math.isfinite(o):
        yield path, o

with httpx.Client(timeout=60.0) as c:
    t = c.post(f"{BASE}/api/v1/auth/login", json={"email": "dukascraper@gmail.com", "password": "Duka@12345"}).json()["access_token"]
    r = c.get(f"{BASE}/api/v1/monitoring/prometheus", headers={"Authorization": f"Bearer {t}"})
    print("status", r.status_code, "bytes", len(r.content))
    if r.status_code == 200:
        d = r.json()
        dur = d.get("http_request_duration_seconds", {})
        print("durations:", len(dur), "requests_total:", len(d.get("http_requests_total", {})))
        bad = list(walk(d))
        print("non-finite values:", bad)
        # Show the routes whose latency exceeded the 10s top bucket.
        at_ceiling = sorted(k for k, v in dur.items() if v == 10.0)
        print("at 10s ceiling:", at_ceiling[:6])
        print("samples:", dict(list(dur.items())[:6]))
    else:
        print(r.text[:400])

# Timing, with the histogram already holding +Inf observations.
with httpx.Client(timeout=120.0) as c:
    t = c.post(f"{BASE}/api/v1/auth/login", json={"email": "dukascraper@gmail.com", "password": "Duka@12345"}).json()["access_token"]
    for i in range(3):
        r = c.get(f"{BASE}/api/v1/monitoring/prometheus", headers={"Authorization": f"Bearer {t}"})
        print(f"call {i}: status={r.status_code} seconds={r.elapsed.total_seconds():.2f} bytes={len(r.content)}")
