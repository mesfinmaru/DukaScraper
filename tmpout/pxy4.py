import asyncio, json, httpx

BASE = "http://127.0.0.1:8000"


async def main():
    async with httpx.AsyncClient(timeout=60.0) as c:
        r = await c.post(
            f"{BASE}/api/v1/auth/login",
            json={"email": "dukascraper@gmail.com", "password": "Duka@12345"},
        )
        print("login", r.status_code)
        tok = r.json()["access_token"]
        hdr = {"Authorization": f"Bearer {tok}"}

        r = await c.get(f"{BASE}/api/v1/monitoring/embed-token", headers=hdr)
        print("embed-token", r.status_code, r.text[:120])
        embed = r.json()["token"]

        # No credential at all
        r = await c.get(f"{BASE}/grafana/__health")
        print("health noauth", r.status_code, r.text[:100])

        r = await c.get(f"{BASE}/grafana/__health", headers=hdr)
        print("health bearer", r.status_code, r.text[:120])

        # Grafana dashboard HTML through the proxy
        r = await c.get(
            f"{BASE}/grafana/d/duka-overview",
            params={"orgId": "1", "token": embed},
            headers={"Referer": "http://localhost:5173/"},
        )
        print("gf dash", r.status_code, len(r.content))
        print("  x-frame-options:", r.headers.get("x-frame-options"))
        body = r.text
        import re
        base_tag = re.findall(r"<base[^>]*>", body)
        print("  base:", base_tag)
        print("  grafana:3000 refs:", body.count("grafana:3000"))
        print("  /grafana refs:", body.count("/grafana"))
        print("  title:", re.findall(r"<title>(.*?)</title>", body))

        # Grafana asset
        m = re.search(r'src="([^"]*\.js[^"]*)"', body)
        if m:
            asset = m.group(1)
            url = f"{BASE}{asset}" if asset.startswith("/") else f"{BASE}/grafana/{asset}"
            r = await c.get(url, params={"token": embed})
            print("  asset", asset[:70], r.status_code, len(r.content))

        # Prometheus
        r = await c.get(f"{BASE}/prometheus/graph", params={"token": embed})
        print("prom graph", r.status_code, r.headers.get("location"), len(r.content))
        r = await c.get(f"{BASE}/prometheus/", params={"token": embed})
        print("prom root", r.status_code, len(r.content), r.headers.get("location"))
        r = await c.get(f"{BASE}/prometheus/targets", params={"token": embed})
        print("prom targets", r.status_code, len(r.content))
        r = await c.get(f"{BASE}/prometheus/alerts", params={"token": embed})
        print("prom alerts", r.status_code, len(r.content))


asyncio.run(main())
