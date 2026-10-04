"""Run the deep worker's production Camoufox config against every challenge page."""
import asyncio, importlib.util, time, sys

spec = importlib.util.spec_from_file_location("deepmain", "/app/workers/deep-worker/main.py")
dm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dm)

SITES = [
    "https://www.scrapingcourse.com/cloudflare-challenge",
    "https://www.scrapingcourse.com/antibot-challenge",
    "https://www.scrapingcourse.com/button-click",
    "https://www.scrapingcourse.com/infinite-scrolling",
    "https://www.scrapingcourse.com/javascript-rendering",
    "https://www.scrapingcourse.com/pagination",
    "https://www.scrapingcourse.com/table-parsing",
    "https://www.scrapingcourse.com/login",
    "https://www.scrapingcourse.com/login/cf-antibot",
    "https://www.scrapingcourse.com/login/cf-turnstile",
    "https://www.scrapingcourse.com/login/csrf",
    "https://www.scrapingcourse.com/ecommerce",
]

async def one(url, profile):
    from camoufox.async_api import AsyncCamoufox
    webgl = dm._get_camoufox_webgl_config(str(profile["os"]), profile["screen"])
    kw = dm._camoufox_launch_kwargs(profile, webgl)
    t0 = time.time()
    try:
        async with AsyncCamoufox(**kw) as b:
            p = await b.new_page()
            r = await p.goto(url, wait_until="domcontentloaded", timeout=60000)
            status = r.status if r else None
            title = await p.title()
            # Wait out an interstitial if one is served.
            while time.time() - t0 < 45:
                if "moment" not in title.lower():
                    break
                await asyncio.sleep(2)
                title = await p.title()
            cleared = "moment" not in title.lower()
            body = (await p.evaluate("() => (document.body ? document.body.innerText : '').slice(0,70).replace(/\s+/g,' ')") or "")
            return status, title, cleared, time.time() - t0, body
    except Exception as e:
        return None, f"ERROR {str(e)[:70]}", False, time.time() - t0, ""

async def main():
    prof = dm._get_camoufox_profile(0)
    print(f"profile: os={prof['os']} locale={prof['locale']} tz={prof.get('timezone')}")
    ok = 0
    for url in SITES:
        status, title, cleared, secs, body = await one(url, prof)
        ok += bool(cleared)
        print(f"  {'OK ' if cleared else 'XX '} {url.split('scrapingcourse.com')[1]:26s} "
              f"status={status} {secs:5.1f}s title={title[:44]!r}", flush=True)
    print(f"==> {ok}/{len(SITES)} loaded without an unresolved interstitial")

asyncio.run(main())
