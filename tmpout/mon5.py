import re

import httpx

BASE = "http://localhost:8000"
r = httpx.post(f"{BASE}/api/v1/auth/login", json={"email": "dukascraper@gmail.com", "password": "Duka@12345"}, timeout=30)
embed = httpx.get(f"{BASE}/api/v1/monitoring/embed-token", headers={"Authorization": f"Bearer {r.json()['access_token']}"}, timeout=30).json()["token"]

dash = httpx.get(f"{BASE}/grafana/api/search", params={"token": embed}, timeout=30)
print("search", dash.status_code, [(d["uid"], d["title"]) for d in dash.json()][:10])

for path in ["/grafana/d/duka-overview?orgId=1&kiosk", "/prometheus/graph?g0.tab=0", "/prometheus/targets", "/prometheus/api/v1/query?query=up"]:
    resp = httpx.get(f"{BASE}{path}", params={"token": embed}, timeout=60)
    body = resp.text[:120].replace("\n", " ")
    print(path, resp.status_code, resp.headers.get("content-type", "")[:40], len(resp.content), "|", body)
    print("   xfo:", resp.headers.get("x-frame-options"))