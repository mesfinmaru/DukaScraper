import httpx
B="http://localhost:8000/api/v1"
t=httpx.post(f"{B}/auth/login", json={"email":"dukascraper@gmail.com","password":"Duka@12345"}).json()["access_token"]
tok=httpx.get(f"{B}/monitoring/embed-token", headers={"Authorization":f"Bearer {t}"}).json()["token"]
# Exactly what the browser iframe does: NO Authorization header, token in query.
for p in [f"/monitoring/grafana/d/duka-overview?orgId=1&refresh=30s&kiosk&token={tok}",
          f"/monitoring/prometheus/graph?g0.tab=0&token={tok}",
          f"/monitoring/grafana/api/health?token={tok}"]:
    r=httpx.get(B+p, timeout=40)
    print(r.status_code, "xframe=", r.headers.get("x-frame-options","(none)"), "len", len(r.content), "|", p[:52])
print("bad token ->", httpx.get(f"{B}/monitoring/grafana/api/health?token=999.abc", timeout=20).status_code)
print("no token  ->", httpx.get(f"{B}/monitoring/grafana/api/health", timeout=20).status_code)
r=httpx.get(B+f"/monitoring/grafana/d/duka-overview?orgId=1&token={tok}")
body=r.text
print("grafana html looks real:", "grafana" in body.lower(), "| has <title>:", "<title>" in body)
