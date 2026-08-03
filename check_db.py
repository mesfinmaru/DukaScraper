# check_db.py
import asyncio

import asyncpg

from app.common.config.settings import settings


async def main():
    db_url = getattr(settings, "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/duka")
    db_url = db_url.replace("postgresql+asyncpg://", "postgresql://").replace("postgres+asyncpg://", "postgres://")

    conn = await asyncpg.connect(db_url)
    rows = await conn.fetch("SELECT source_job_id, language, character_count FROM parsed_items;")
    print("🐘 Postgres Records:", [dict(r) for r in rows])
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
