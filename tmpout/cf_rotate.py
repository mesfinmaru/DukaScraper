"""Does profile rotation clear a challenge that the first profile missed?"""
import asyncio, importlib.util, time, sys
spec = importlib.util.spec_from_file_location("deepmain", "/app/workers/deep-worker/main.py")
dm = importlib.util.module_from_spec(spec); spec.loader.exec_module(dm)

URL = "https://www.scrapingcourse.com/antibot-challenge"

async def one(i, patience=35.0):
    from camoufox.async_api import AsyncCamoufox
    prof = dm._get_camoufox_profile(i)
    kw = dm._camoufox_launch_kwargs(prof, dm._get_camoufox_webgl_config(str(prof["os"]), prof["screen"]))
    t0 = time.time()
    async with AsyncCamoufox(**kw) as b:
        p = await b.new_page()
        await p.goto(URL, wait_until="domcontentloaded", timeout=60000)
        while time.time() - t0 < patience:
            t = await p.title()
            if "moment" not in t.lower():
                print(f"  attempt {i+1} os={prof['os']:8s} tz={prof['timezone']:18s} PASS {time.time()-t0:5.1f}s title={t[:44]!r}", flush=True)
                return True
            await asyncio.sleep(2)
        print(f"  attempt {i+1} os={prof['os']:8s} tz={prof['timezone']:18s} fail {time.time()-t0:5.1f}s", flush=True)
        return False

async def main():
    for i in range(3):
        if await one(i):
            print("==> cleared by rotation"); return
    print("==> still not cleared after 3 profiles")

asyncio.run(main())
