"""Measure pass rate of the current worker-style launch vs candidate fixes."""
import asyncio, time, sys
from camoufox.utils import Screen

URL = "https://www.scrapingcourse.com/cloudflare-challenge"
REPS = int(sys.argv[2]) if len(sys.argv) > 2 else 3

# Exactly what the worker does today for attempt 0 -> profile 0 (windows).
WORKER_PROFILE = dict(
    os="windows",
    locale="en-US",
    screen=Screen(max_width=1920, max_height=1080),
    window=(1920, 1040),
)

CANDIDATES = {
    "baseline":        dict(**WORKER_PROFILE),
    "tz-config":       dict(**WORKER_PROFILE, config={"timezone": "Africa/Nairobi"}),
    "geoip":           dict(**WORKER_PROFILE, geoip=True),
}

async def one(label, patience=70.0, **kw):
    from camoufox.async_api import AsyncCamoufox
    t0 = time.time()
    try:
        async with AsyncCamoufox(headless=False, humanize=True, **kw) as b:
            p = await b.new_page()
            await p.goto(URL, wait_until="domcontentloaded", timeout=60000)
            while time.time() - t0 < patience:
                st = await p.evaluate("() => ({t: document.title, tz: Intl.DateTimeFormat().resolvedOptions().timeZone, w: document.querySelectorAll('iframe[src*=\"challenges.cloudflare.com\"]').length})")
                if "moment" not in st["t"].lower():
                    return True, time.time() - t0, st
                await asyncio.sleep(2.5)
            return False, time.time() - t0, st
    except Exception as e:
        return False, time.time() - t0, {"err": str(e)[:120]}

async def main():
    which = sys.argv[1].split(",") if len(sys.argv) > 1 else ["baseline"]
    for name in which:
        wins = 0
        for i in range(REPS):
            ok, secs, st = await one(name, **CANDIDATES[name])
            wins += ok
            print(f"  {name:12s} run {i+1}/{REPS}: {'PASS' if ok else 'fail'} {secs:5.1f}s tz={st.get('tz')} w={st.get('w')} {st.get('err','')}", flush=True)
        print(f"==> {name}: {wins}/{REPS} passed", flush=True)

asyncio.run(main())
