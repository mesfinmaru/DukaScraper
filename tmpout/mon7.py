import re

import httpx

BASE = "http://localhost:8000"
r = httpx.post(f"{BASE}/api/v1/auth/login", json={"email": "dukascraper@gmail.com", "password": "Duka@12345"}, timeout=30)
embed = httpx.get(f"{BASE}/api/v1/monitoring/embed-token", headers={"Authorization": f"Bearer {r.json()['access_token']}"}, timeout=30).json()["token"]

h = httpx.get(f"{BASE}/grafana/d/duka-overview", params={"orgId": 1, "kiosk": "", "token": embed}, timeout=60)
t = h.text
for key in ["grafanaBootData", "window.grafanaBootData", "appUrl", "appSubUrl", "bootData", "settings ="]:
    print(key, "count:", t.count(key))
i = t.find("appSubUrl")
print(t[max(0, i - 400):i + 200].replace("\n", " ") if i > 0 else "no appSubUrl")
j = t.find("grafanaBootData")
print("---")
print(t[max(0, j - 200):j + 400].replace("\n", " ") if j > 0 else "no bootdata")