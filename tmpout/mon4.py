import httpx

c = httpx.Client(timeout=30, follow_redirects=False)
for url in [
    "http://grafana:3000/login",
    "http://grafana:3000/grafana/login",
    "http://grafana:3000/grafana/d/duka-overview?orgId=1",
    "http://grafana:3000/grafana/api/health",
]:
    r = c.get(url, headers={"X-Forwarded-Host": "localhost:8000", "X-Forwarded-Proto": "http"})
    print(url, r.status_code, "->", r.headers.get("location"), len(r.content))