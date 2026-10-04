import json
import re

import httpx

BASE = "http://localhost:8000"
r = httpx.post(
    f"{BASE}/api/v1/auth/login",
    json={"email": "dukascraper@gmail.com", "password": "Duka@12345"},
    timeout=30,
)
r.raise_for_status()
tok = r.json()["access_token"]

tr = httpx.get(f"{BASE}/api/v1/monitoring/embed-token", headers={"Authorization": f"Bearer {tok}"}, timeout=30)
print("embed-token", tr.status_code)
embed = tr.json()["token"]
cookie = tr.cookies.get("duka_embed")
print("cookie set:", bool(cookie))

for path in ["/grafana/__health", "/grafana/login", "/grafana/d/duka-overview?orgId=1&kiosk"]:
    resp = httpx.get(f"{BASE}{path}", params={"token": embed}, timeout=60)
    print(path, resp.status_code, "len", len(resp.content))
    if resp.headers.get("content-type", "").startswith("text/html"):
        print("  x-frame-options:", resp.headers.get("x-frame-options"))
        for m in re.finditer(r'(base href="[^"]*"|appUrl[^,]{0,80}|bootData|public/build/[^"]*)', resp.text):
            print("   ", m.group(0)[:120])
        print("  grafana:3000 refs:", resp.text.count("grafana:3000"))

# assets
h = httpx.get(f"{BASE}/grafana/login", params={"token": embed}, timeout=60)
paths = re.findall(r'(?:src|href)="([^"]+)"', h.text)
print("asset refs:", paths[:8])
for p in paths[:4]:
    if p.startswith("http"):
        continue
    a = httpx.get(f"{BASE}{p}" if p.startswith("/") else f"{BASE}/grafana/{p}", params={"token": embed}, timeout=60)
    print("  asset", p, a.status_code, len(a.content), a.headers.get("content-type"))