"""Exercise the deep worker's REAL launch-kwargs path, with the auto-geoip logic.

For each fallback attempt index (0..2) this builds the kwargs exactly as
`_try_camoufox_bypass` would, prints which timezone decision it made, and then
navigates to the live challenge to see whether it clears.

Run inside the deep-worker container:
    docker exec -e DISPLAY=:99 deep-worker python /app/tmpout/cf_auto.py
"""
import asyncio, importlib.util, time

spec = importlib.util.spec_from_file_location("deepmain", "/app/workers/deep-worker/main.py")
dm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dm)

URL = "https://www.scrapingcourse.com/cloudflare-challenge"


async def attempt(idx):
    from camoufox.async_api import AsyncCamoufox
    profile = dm._get_camoufox_profile(idx)
    webgl = dm._get_camoufox_webgl_config(str(profile["os"]), profile["screen"])
    kw = dm._camoufox_launch_kwargs(profile, webgl, idx)
    shown = "geoip" if kw.get("geoip") else (kw.get("config") or {}).get("timezone", "?")
    t0 = time.time()
    try:
        async with AsyncCamoufox(**kw) as b:
            p = await b.new_page()
            await p.goto(URL, wait_until="domcontentloaded", timeout=60000)
            while time.time() - t0 < 45:
                if "moment" not in (await p.title()).lower():
                    print(f"  attempt {idx} (os={profile['os']:7s}) PASS "
                          f"{time.time()-t0:5.1f}s  tz={shown}", flush=True)
                    return True
                await asyncio.sleep(2)
            print(f"  attempt {idx} (os={profile['os']:7s}) fail "
                  f"{time.time()-t0:5.1f}s  tz={shown}", flush=True)
            return False
    except Exception as e:
        print(f"  attempt {idx} (os={profile['os']:7s}) ERROR tz={shown}: {str(e)[:150]}",
              flush=True)
        return False


async def main():
    from app.common.config.settings import settings
    mode = settings.browser_geoip_mode()
    print(f"mode={mode!r} DEEP_BROWSER_GEOIP={settings.DEEP_BROWSER_GEOIP!r}")
    print(f"geoip usable = {dm.camoufox_geoip_usable()}")
    wins = 0
    for idx in range(3):
        wins += await attempt(idx)
    print(f"==> {wins}/3 attempts cleared the interstitial", flush=True)


asyncio.run(main())
