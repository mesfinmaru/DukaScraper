import httpx

BASE = "http://localhost:8000"
r = httpx.post(f"{BASE}/api/v1/auth/login", json={"email": "dukascraper@gmail.com", "password": "Duka@12345"}, timeout=30)
embed = httpx.get(f"{BASE}/api/v1/monitoring/embed-token", headers={"Authorization": f"Bearer {r.json()['access_token']}"}, timeout=30).json()["token"]

for path in ["/grafana/login", "/grafana/d/duka-overview?orgId=1"]:
    resp = httpx.get(f"{BASE}{path}", params={"token": embed}, timeout=60)
    print(path, resp.status_code, "->", resp.headers.get("location"), "|", resp.text[:200].replace("\n", " "))

# follow manually
resp = httpx.get(f"{BASE}{path}", params={"token": embed}, follow_redirects=True, timeout=60)
print("final", resp.status_code, resp.url, len(resp.content))