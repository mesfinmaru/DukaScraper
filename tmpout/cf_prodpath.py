"""Exercise the deep worker's REAL launch-kwargs path against the live challenge."""
import asyncio, importlib.util, time, sys

spec = importlib.util.spec_from_file_location("deepmain", "/app/workers/deep-worker/main.py")
dm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dm)

URL = "https://www.scrapingcourse.com/cloudflare-challenge"

async def attempt(profile, label, patience=45.0):
    from camoufox.async_api import AsyncCamoufox
    webgl = dm._get_camoufox_webgl_config(str(profile["os"]), profile["screen"])
    kw = dm._camoufox_launch_kwargs(profile, webgl)
    shown = "geoip" if kw.get("geoip") else (kw.get("config") or {}).get("timezone")
    t0 = time.time()
    try:
        async with AsyncCamoufox(**kw) as b:
            p = await b.new_page()
            await p.goto(URL, wait_until="domcontentloaded", timeout=60000)
            while time.time() - t0 < patience:
                if "moment" not in (await p.title()).lower():
                    print(f"  {label:22s} PASS {time.time()-t0:5.1f}s  tz={shown}", flush=True)
                    return True
                await asyncio.sleep(2)
            print(f"  {label:22s} fail {time.time()-t0:5.1f}s  tz={shown}", flush=True)
            return False
    except Exception as e:
        print(f"  {label:22s} ERROR tz={shown}: {str(e)[:130]}", flush=True)
        return False

async def main():
    from app.common.config.settings import settings
    mode = sys.argv[1]
    if mode == "nogeoip":
        settings.DEEP_BROWSER_GEOIP = False
        dm.camoufox_geoip_usable.__globals__["_geoip_probe_result"] = False
    print(f"mode={mode} DEEP_BROWSER_GEOIP={settings.DEEP_BROWSER_GEOIP} fallback_tz={settings.browser_timezone()}")
    wins = 0
    for i in range(3):
        prof = dm._get_camoufox_profile(i)
        wins += await attempt(prof, f"attempt {i+1} os={prof['os']}")
    print(f"==> {mode}: {wins}/3 passed")

asyncio.run(main())
