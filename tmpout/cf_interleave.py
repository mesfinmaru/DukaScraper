"""Interleave baseline and candidate so IP-reputation drift cannot explain the gap."""
import asyncio, time, sys
from camoufox.utils import Screen

URL = "https://www.scrapingcourse.com/cloudflare-challenge"
PROFILE = dict(os="windows", locale="en-US",
               screen=Screen(max_width=1920, max_height=1080), window=(1920, 1040))

async def one(label, patience=70.0, **kw):
    from camoufox.async_api import AsyncCamoufox
    t0 = time.time()
    try:
        async with AsyncCamoufox(headless=False, humanize=True, **kw) as b:
            p = await b.new_page()
            await p.goto(URL, wait_until="domcontentloaded", timeout=60000)
            while time.time() - t0 < patience:
                t = await p.evaluate("() => document.title")
                if "moment" not in t.lower():
                    return True, time.time() - t0
                await asyncio.sleep(2.0)
            return False, time.time() - t0
    except Exception as e:
        return False, time.time() - t0

async def main():
    order = sys.argv[1].split(",")
    results = {}
    for name in order:
        kw = dict(PROFILE)
        if name == "tz": kw["config"] = {"timezone": "Africa/Nairobi"}
        elif name == "empty": kw["config"] = {}
        elif name == "tz-utc": kw["config"] = {"timezone": "UTC"}
        ok, secs = await one(name, **kw)
        results.setdefault(name, []).append(ok)
        print(f"  {name:6s} {'PASS' if ok else 'fail'} {secs:5.1f}s", flush=True)
    print()
    for k, v in results.items():
        print(f"==> {k}: {sum(v)}/{len(v)} passed")

asyncio.run(main())
