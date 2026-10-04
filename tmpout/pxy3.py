import httpx, re
B="http://localhost:8000/api/v1"
c=httpx.Client()
t=c.post(f"{B}/auth/login", json={"email":"dukascraper@gmail.com","password":"Duka@12345"}).json()["access_token"]
r=c.get(f"{B}/monitoring/embed-token", headers={"Authorization":f"Bearer {t}"})
print("cookie set:", [f"{k}={v[:22]}..." for k,v in r.cookies.items()])
html=c.get(f"{B}/monitoring/grafana/d/duka-overview?orgId=1&kiosk").text
# Pull a real asset URL out of the page and fetch it with ONLY the cookie
# (no query token) - exactly what the browser does for a sub-resource.
m=re.search(r'(/public/build/[\w\.\-]+\.js)', html) or re.search(r'((?:/public|/assets)/[\w\.\-/]+\.js)', html)
print("asset found:", m.group(1) if m else None)
if m:
    a=c.get(f"{B}/monitoring/grafana{m.group(1)}")
    print("  asset status:", a.status_code, "len", len(a.content), "ct", a.headers.get("content-type"))
