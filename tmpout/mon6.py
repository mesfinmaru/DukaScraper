import json
import re

import httpx

BASE = "http://localhost:8000"
r = httpx.post(f"{BASE}/api/v1/auth/login", json={"email": "dukascraper@gmail.com", "password": "Duka@12345"}, timeout=30)
embed = httpx.get(f"{BASE}/api/v1/monitoring/embed-token", headers={"Authorization": f"Bearer {r.json()['access_token']}"}, timeout=30).json()["token"]

h = httpx.get(f"{BASE}/grafana/d/duka-overview", params={"orgId": 1, "kiosk": "", "token": embed}, timeout=60)
m = re.search(r"window\.grafanaBootData\.settings = (\{.*?\});", h.text, re.S)
print("bootdata found:", bool(m))
if m:
    s = json.loads(m.group(1))
    for k in sorted(s):
        if any(t in k.lower() for t in ("url", "subpath", "subpath", "route", "build", "anonymous", "login", "dashboards")):
            print("  ", k, "=", s[k])
print("index refs:", h.text[:300].replace("\n", " "))