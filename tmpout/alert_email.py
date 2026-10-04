import sys, asyncio, smtplib
from email.message import EmailMessage
sys.path.insert(0,'/app')
from app.services.alert_service import (build_alert_email, Alert, notify_alert_email,
    record_alert)
from app.storage.postgres.client import pg_client
from app.common.config.settings import settings

a = Alert(alert_id="ALR00000000001001", job_id="JOB1791087313758", item_id="HTTPITEM0",
  url="https://example.com/0", title="የጥቃት ማስጠንቀቂያ", category="physical_threat",
  severity=5, language="am", summary="በአዲስ አበባ ከተማሪ የተከሰተ አደጋ ተወጭቷል።",
  entities=["TPLF","አዲስ አበባ"], analysis_source="llm", llm_model="openai/gpt-oss-120b",
  created_at=None)
subj, body = build_alert_email(a, settings.UI_BASE_URL)
print("SUBJECT:", subj); print("---- BODY ----"); print(body); print("---- END ----")
print("alert emails enabled:", settings.ALERT_EMAILS_ENABLED, "| ui:", settings.UI_BASE_URL)

async def main():
    await pg_client.connect()
    print("notify ->", await notify_alert_email("JOB1791087313758","ALR00000000001001"))
asyncio.run(main())
