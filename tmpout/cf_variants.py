"""Try Camoufox variants against the real challenge; report which (if any) clears."""
import asyncio, time, sys

URL = "https://www.scrapingcourse.com/cloudflare-challenge"

async def attempt(label, patience=75.0, **kw):
    from camoufox.async_api import AsyncCamoufox
    t0 = time.time()
    st = {"tz": "?", "widgets": 0, "t": "?"}
    try:
        async with AsyncCamoufox(headless=False, humanize=True, **kw) as b:
            p = await b.new_page()
            await p.goto(URL, wait_until="domcontentloaded", timeout=60000)
            while time.time() - t0 < patience:
                st = await p.evaluate("""() => ({
                    t: document.title,
                    widgets: document.querySelectorAll('iframe[src*="challenges.cloudflare.com"], .cf-turnstile').length,
                    tz: Intl.DateTimeFormat().resolvedOptions().timeZone,
                })""")
                if "moment" not in st["t"].lower():
                    print(f"[{label}] CLEARED in {time.time()-t0:.1f}s title={st['t']!r} tz={st['tz']}")
                    return True
                await asyncio.sleep(3)
            cookies = await p.context.cookies()
            print(f"[{label}] FAILED {patience:.0f}s tz={st['tz']} widgets={st['widgets']} "
                  f"cf={[c['name'] for c in cookies if 'cf' in c['name'].lower()]}")
            return False
    except Exception as e:
        print(f"[{label}] ERROR {str(e)[:200]}")
        return False

async def main():
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which in ("all", "tz"):
        await attempt("tz=Africa/Nairobi", config={"timezone": "Africa/Nairobi"}, locale="en-US", os="windows")
    if which in ("all", "geoip"):
        await attempt("geoip=True", geoip=True)
    if which in ("all", "profile"):
        await attempt("worker-style profile", os="macos", locale="en-GB", screen=None)

asyncio.run(main())
