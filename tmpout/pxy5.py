import asyncio, re, httpx

BASE = "http://127.0.0.1:8000"


async def main():
    async with httpx.AsyncClient(timeout=60.0) as c:
        t = (await c.post(
            f"{BASE}/api/v1/auth/login",
            json={"email": "dukascraper@gmail.com", "password": "Duka@12345"},
        )).json()["access_token"]
        e = (
            await c.get(
                f"{BASE}/api/v1/monitoring/embed-token",
                headers={"Authorization": f"Bearer {t}"},
            )
        ).json()["token"]

        r = await c.get(f"{BASE}/prometheus/targets", params={"token": e})
        print("targets", r.status_code, len(r.content))
        assets = sorted(set(re.findall(r'(?:src|href)="([^"]+)"', r.text)))
        print("assets:", assets)
        print("body starts:", " ".join(r.text[:200].split()))

        for a in assets:
            path = a[2:] if a.startswith("./") else a.lstrip("/")
            url = f"{BASE}/prometheus/{path}"
            r = await c.get(url, params={"token": e})
            print("  asset", url.replace(BASE, ""), r.status_code, len(r.content),
                  r.headers.get("content-type"))

        r = await c.get(f"{BASE}/prometheus/api/v1/status/buildinfo", params={"token": e})
        print("api buildinfo", r.status_code, r.text[:160])


asyncio.run(main())
