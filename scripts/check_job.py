"""Check what URLs were crawled for JOB00000066."""
import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect(host='localhost', port=5432, user='postgres', password='postgres', database='duka_db')
    
    # Check crawl_log schema
    cols = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_name='crawl_log' ORDER BY ordinal_position")
    print("crawl_log columns:", [c['column_name'] for c in cols])
    
    # Check parsed_items schema
    cols2 = await conn.fetch("SELECT column_name FROM information_schema.columns WHERE table_name='parsed_items' ORDER BY ordinal_position")
    print("parsed_items columns:", [c['column_name'] for c in cols2])
    
    rows = await conn.fetch("SELECT url, event_type, status FROM crawl_log WHERE job_id = $1 ORDER BY created_at", 'JOB00000066')
    for r in rows:
        print(f"  crawl_log: {r['status']:12s} {r['event_type']:30s} {r['url']}")
    
    items = await conn.fetch("SELECT source_url, character_count FROM parsed_items WHERE job_id = $1", 'JOB00000066')
    print(f"\n--- parsed_items ({len(items)}): ---")
    for r in items:
        print(f"  chars={r['character_count']:6d}  {r['source_url']}")
    
    # Also check what child jobs exist
    jobs = await conn.fetch("SELECT job_id, url, status FROM jobs WHERE job_id LIKE 'JOB%' ORDER BY created_at DESC LIMIT 15", )
    print(f"\n--- Recent jobs ({len(jobs)}): ---")
    for j in jobs:
        print(f"  {j['job_id']} {j['status']:12s} {j['url']}")
    
    await conn.close()

asyncio.run(main())
