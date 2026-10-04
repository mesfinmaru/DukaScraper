"""Standalone probe: does the production Camoufox path still clear /login/cf-turnstile alone?"""
import asyncio, importlib.util, time

spec = importlib.util.spec_from_file_location("deepmain", "/app/workers/deep-worker/main.py")
dm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dm)

URL = "https://www.scrapingcourse.com/login/cf-turnstile"


async def one(attempt: int, budget: float = 90.0):
    from camoufox.async_api import AsyncCamoufox
    prof = dm._get_camoufox_profile(attempt)
    webgl = dm._get_camoufox_webgl_config(str(prof["os"]), prof["screen"])
    kw = dm._camoufox_launch_kwargs(prof, webgl, attempt)
    tz = "geoip" if kw.get("geoip") else (kw.get("config") or {}).get("timezone")
    t0 = time.time()
    cleared_at = None
    clicked = False
    status = None
    err = ""
    try:
        async with AsyncCamoufox(**kw) as b:
            p = await b.new_page()
            r = await p.goto(URL, wait_until="domcontentloaded", timeout=60000)
            status = r.status if r else None
            while time.time() - t0 < budget:
                title = (await p.title() or "")
                if "moment" not in title.lower() and time.time() - t0 > 3:
                    cleared_at = time.time() - t0
                    break
                # try a single widget click once, 10s in
                if not clicked and time.time() - t0 >= 10:
                    try:
                        el = await p.query_selector(
                            "iframe[src*='challenges.cloudflare.com'], iframe[title*='Cloudflare']"
                        )
                        if el:
                            box = await el.bounding_box()
                            if box and box["width"] >= 10:
                                x = box["x"] + min(30.0, box["width"] / 2)
                                y = box["y"] + box["height"] / 2
                                await p.mouse.click(x, y)
                                clicked = True
                    except Exception:
                        pass
                await asyncio.sleep(1.0)
            title = await p.title()
            body = (await p.evaluate(
                "() => (document.body ? document.body.innerText : '').slice(0,90).replace(/\\s+/g,' ')"
            ) or "")
            cookies = [c["name"] for c in await p.context.cookies()]
            return status, title, cleared_at, clicked, body, cookies, time.time() - t0
    except Exception as e:
        err = f"{type(e).__name__}: {e}"[:160]
        return status, f"ERROR {err}", cleared_at, clicked, "", [], time.time() - t0


async def main():
    for attempt in (0, 1):
        status, title, cleared_at, clicked, body, cookies, secs = await one(attempt)
        print(f"attempt={attempt} status={status} cleared_at={cleared_at} clicked={clicked} "
              f"elapsed={secs:.1f}s clearance={'cf_clearance' in cookies}\n"
              f"  title={title[:70]!r}\n  body={body!r}", flush=True)

asyncio.run(main())
