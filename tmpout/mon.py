import httpx
B="http://localhost:8000/api/v1"
t=httpx.post(f"{B}/auth/login", json={"email":"dukascraper@gmail.com","password":"Duka@12345"}).json()["access_token"]
h={"Authorization":f"Bearer {t}"}
for ep in ["/monitoring/health","/monitoring/urls","/monitoring/prometheus"]:
    try:
        r=httpx.get(f"{B}{ep}", headers=h, timeout=25)
        print(ep, "->", r.status_code, r.text[:220].replace("\n"," "))
    except Exception as e: print(ep, "ERR", type(e).__name__, str(e)[:100])
