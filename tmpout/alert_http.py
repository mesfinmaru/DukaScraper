import httpx, asyncio
B="http://localhost:8000/api/v1"
t=httpx.post(f"{B}/auth/login", json={"email":"dukascraper@gmail.com","password":"Duka@12345"}).json()["access_token"]
h={"Authorization":f"Bearer {t}"}
import sys; sys.path.insert(0,'/app')

async def main():
    from app.services.alert_service import record_alert
    from app.storage.postgres.client import pg_client
    await pg_client.connect()
    for i,sev in enumerate([5,4,5]):
        await record_alert(job_id="JOB1791087313758", item_id=f"HTTPITEM{i}",
            url=f"https://example.com/{i}", title=f"Alert {i}", category="physical_threat",
            severity=sev, language="am", summary=f"Summary {i}", entities=["X"])
    r=httpx.get(f"{B}/alerts", headers=h, params={"limit":10}); d=r.json()
    print("GET /alerts ->", r.status_code, "total", d["total"], "has_more", d["has_more"])
    for a in d["alerts"]: print("   ", a["alert_id"], a["severity"], a["priority"], a["read"], a["short_summary"])
    r=httpx.get(f"{B}/alerts/unread", headers=h); print("GET /alerts/unread ->", r.status_code, r.json())
    aid=d["alerts"][0]["alert_id"]
    r=httpx.post(f"{B}/alerts/{aid}/read", headers=h); print("POST read ->", r.status_code, r.json())
    print("unread after one:", httpx.get(f"{B}/alerts/unread", headers=h).json())
    r=httpx.post(f"{B}/alerts/read-all", headers=h); print("POST read-all ->", r.status_code, r.json())
    print("unread after all:", httpx.get(f"{B}/alerts/unread", headers=h).json())
    r=httpx.post(f"{B}/alerts/NOPE/read", headers=h); print("POST bad id ->", r.status_code, r.text[:60])
    r=httpx.get(f"{B}/alerts", params={"limit":5}); print("unauthenticated ->", r.status_code)
    r=httpx.get(f"{B}/alerts/unread-only-check", headers=h); print("404 check ->", r.status_code)
asyncio.run(main())
