import httpx, sys
B="http://localhost:8000/api/v1"
t=httpx.post(f"{B}/auth/login", json={"email":"dukascraper@gmail.com","password":"Duka@12345"}).json()["access_token"]
h={"Authorization":f"Bearer {t}"}
et=httpx.get(f"{B}/monitoring/embed-token", headers=h); print("embed-token:", et.status_code, et.text[:80])
tok=et.json().get("token","")
for path in ["/monitoring/grafana/d/duka-overview?orgId=1", f"/monitoring/grafana/d/duka-overview?orgId=1&token={tok}",
             "/monitoring/prometheus/api/v1/query?query=up"]:
    r=httpx.get(B+path, headers=h, timeout=40)
    print(path[:56], "->", r.status_code, "x-frame:", r.headers.get("x-frame-options","(stripped)"), "len", len(r.content))
r=httpx.get(f"{B}/monitoring/grafana/d/duka-overview", timeout=30); print("no token ->", r.status_code)
