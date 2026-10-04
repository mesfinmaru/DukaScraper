import httpx, asyncio, json
B="http://localhost:8000/api/v1"
t=httpx.post(f"{B}/auth/login", json={"email":"dukascraper@gmail.com","password":"Duka@12345"}).json()["access_token"]
h={"Authorization":f"Bearer {t}"}

from app.services.alert_service import record_alert, list_alerts, unread_count, mark_read, mark_all_read
from app.storage.postgres.client import pg_client

async def main():
    await pg_client.connect()
    uid = "USRADMIN"  # placeholder, resolved below
    job = await pg_client.get_job("JOB1791087313758")
    print("job:", job["job_id"], job["user_id"], job["status"])
    me = job["user_id"]

    aid = await record_alert(job_id=job["job_id"], item_id="TESTITEM001",
        url="https://example.com/x", title="የጥቃት ማስጠንቀቂያ", category="physical_threat",
        severity=5, language="am", summary="በአዲስ አበባ ከተማሪ የተከሰተ አደጋ", entities=["TPLF","አዲስ አበባ"])
    print("raised:", aid)
    # severity 3 must NOT alert
    low = await record_alert(job_id=job["job_id"], item_id="TESTITEM002", url="https://e.com/y", severity=3)
    print("severity 3 raised (expect None):", low)

    ids=None  # admin scope for the test
    rows,total = await list_alerts(user_id=me, job_ids=None, limit=10, offset=0)
    print("feed total:", total, "unread:", await unread_count(me, None))
    for a in rows: print("  ", a.alert_id, a.severity, a.title[:30], "read=",a.read)

    aid0 = rows[0].alert_id
    print("mark one:", await mark_read(me, aid0), "-> unread now", await unread_count(me,None))
    print("mark all:", await mark_all_read(me, None), "-> unread now", await unread_count(me,None))
    print("idempotent mark_all again:", await mark_all_read(me, None))

asyncio.run(main())
