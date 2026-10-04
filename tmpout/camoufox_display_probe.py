"""Does sharing one Xvfb display break concurrent Camoufox logins?

All Camoufox fallbacks in the deep worker run headful on DISPLAY=:99. With
several jobs in flight their windows stack at identical coordinates, and
Playwright's hit-target check can reject a click aimed at a covered element
("ElementHandle.click: Timeout 30000ms exceeded"). That is the difference
between one user's crawl logging in and five users' crawls logging in.

Runs the real portal login for scrapingcourse.com in N concurrent Camoufox
browsers, twice: once all on the shared display, once each on its own Xvfb.
"""
import asyncio
import importlib.util
import json
import os
import subprocess
import sys
import time

spec = importlib.util.spec_from_file_location("deepmain", "/app/workers/deep-worker/main.py")
dm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dm)

CREDS = {"username": "admin@example.com", "password": "password"}
LOGIN_URL = "https://www.scrapingcourse.com/login"
CONFIG = "/app/configs/domains/scrapingcourse.json"


def own_display(base: int) -> str:
    """Start a private Xvfb and return its DISPLAY value."""
    for n in range(base, base + 40):
        if os.path.exists(f"/tmp/.X11-unix/X{n}") or os.path.exists(f"/tmp/.X{n}-lock"):
            continue
        proc = subprocess.Popen(
            ["Xvfb", f":{n}", "-screen", "0", "1920x1080x24", "-ac",
             "+extension", "GLX", "+render", "-noreset"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        for _ in range(40):
            time.sleep(0.2)
            if os.path.exists(f"/tmp/.X11-unix/X{n}"):
                return f":{n}"
        proc.kill()
    raise RuntimeError("no free display")


async def one_login(display: str, idx: int) -> dict:
    """Launch Camoufox on *display* and run the real portal login."""
    os.environ["DISPLAY"] = display
    for k, v in dm.DeepWorker._camoufox_env().items():
        os.environ[k] = v
    t0 = time.time()
    out = {"idx": idx, "display": display, "login": False, "err": ""}
    try:
        from camoufox.async_api import AsyncCamoufox
        from app.services.portal_handler import PortalConfig, PortalHandler

        prof = dm._get_camoufox_profile(0)
        webgl = dm._get_camoufox_webgl_config(str(prof["os"]), prof["screen"])
        kw = dm._camoufox_launch_kwargs(prof, webgl, 0)
        async with AsyncCamoufox(**kw) as b:
            page = await b.new_page()
            await page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(2)
            cfg = PortalConfig.from_dict(json.load(open(CONFIG, encoding="utf-8")))
            res = await PortalHandler(page, cfg).run(CREDS)
            out["login"] = bool(res.get("login_successful"))
            out["url_after"] = page.url
    except Exception as exc:
        out["err"] = f"{type(exc).__name__}: {exc}"[:180]
    out["secs"] = round(time.time() - t0, 1)
    return out


async def scenario(label: str, display_fn):
    os.environ["DISPLAY"] = ":99"
    dm.DeepWorker._xvfb_ensure("probe")
    displays = [display_fn(i) for i in range(2)]
    print(f"\n=== {label} (displays={displays}) ===", flush=True)
    t0 = time.time()
    results = await asyncio.gather(*(one_login(d, i) for i, d in enumerate(displays)))
    for r in sorted(results, key=lambda r: r["idx"]):
        print(
            f"  job{r['idx']} display={r['display']} login={r['login']} "
            f"url={r.get('url_after','')[:60]} {r['secs']}s err={r['err']}",
            flush=True,
        )
    ok = sum(1 for r in results if r["login"])
    print(f"  => {ok}/2 logins succeeded in {time.time()-t0:.1f}s", flush=True)
    return ok


async def main():
    shared = await scenario("BOTH ON SHARED :99", lambda i: ":99")
    await asyncio.sleep(3)
    n = [100]

    def _next(i):
        d = own_display(n[0])
        n[0] += 1
        return d

    dedicated = await scenario("SEPARATE DISPLAYS", _next)
    print(f"\nSUMMARY shared={shared}/2 dedicated={dedicated}/2", flush=True)


asyncio.run(main())