"""Probe v2: Patchright vs Camoufox on /login/cf-turnstile.

Adds: pageerror capture, frame URL tracking, longer poll window, turnstile
API presence, widget wrapper classes (error/loading state), and a screenshot
so the widget's visual state can be inspected.
"""
import asyncio
import importlib.util
import time

URL = "https://www.scrapingcourse.com/login/cf-turnstile"

spec = importlib.util.spec_from_file_location("deepmain", "/app/workers/deep-worker/main.py")
dm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dm)

WIDGET_JS = """() => {
  const d = document.querySelector('.cf-turnstile') || document.getElementById('waf');
  const resp = Array.from(document.querySelectorAll('input[name="cf-turnstile-response"]'));
  return {
    api: typeof window.turnstile,
    wrapperClass: d ? d.className : null,
    childTags: d ? Array.from(d.children).map(c => c.tagName + '.' + (c.className||'') + '#' + (c.id||'')) : [],
    tokenLens: resp.map(i => (i.value || '').trim().length),
    frames: Array.from(document.querySelectorAll('iframe')).length,
    text: (document.body ? document.body.innerText : '').replace(/\\s+/g, ' ').slice(0, 120),
  };
}"""


async def probe(label, page, budget=100.0):
    errs, cframes = [], []
    page.on("pageerror", lambda e: errs.append(f"{type(e).__name__}: {e}"[:200]))
    page.on("framenavigated",
            lambda f: cframes.append(f"{f.name or '(anon)'} -> {f.url[:110]}") if "challenges.cloudflare.com" in f.url else None)
    t0 = time.time()
    try:
        r = await page.goto(URL, wait_until="domcontentloaded", timeout=60000)
        print(f"\n=== {label} === nav status={r.status if r else None} in {time.time()-t0:.1f}s", flush=True)
    except Exception as exc:
        print(f"\n=== {label} === NAV FAILED {type(exc).__name__}: {exc}", flush=True)
        return
    token_at = None
    while time.time() - t0 < budget:
        await asyncio.sleep(2.0)
        try:
            w = await page.evaluate(WIDGET_JS)
        except Exception as exc:
            w = {"err": f"{type(exc).__name__}"}
        lens = w.get("tokenLens") or []
        got = any(n > 0 for n in lens)
        el = time.time() - t0
        if el < 14 or el % 10 < 2.1 or got:
            print(f"  t+{el:5.1f}s api={w.get('api')} frames={w.get('frames')} "
                  f"lens={lens} cls={w.get('wrapperClass')} "
                  f"kids={w.get('childTags')}", flush=True)
        if got:
            token_at = el
            print(f"  *** TOKEN ACQUIRED at t+{el:.1f}s ***", flush=True)
            break
        # try one click at t+25s
        if 25 <= el <= 27:
            print("  (clicking widget once at t+25s)", flush=True)
            try:
                await dm._click_turnstile_widget(page)
            except Exception as exc:
                print(f"  click error: {type(exc).__name__}: {exc}", flush=True)
    # frame internals
    print("  --- frames ---", flush=True)
    try:
        for fr in page.frames:
            if "challenges.cloudflare.com" not in fr.url:
                continue
            print(f"   url={fr.url[:130]}", flush=True)
            try:
                body = await fr.evaluate("() => (document.body ? document.body.innerHTML : 'NO BODY')")
                print(f"   innerHTML({len(body or '')}): {(body or '')[:400]}", flush=True)
            except Exception as exc:
                print(f"   frame eval failed: {type(exc).__name__}: {exc}"[:200], flush=True)
    except Exception as exc:
        print(f"   frame walk failed: {exc}"[:200], flush=True)
    print(f"  token_at={token_at} elapsed={time.time()-t0:.1f}s", flush=True)
    print("  pageerrors:", errs[:6] if errs else "none", flush=True)
    print("  cf frame navigations:", cframes[-6:] if cframes else "none", flush=True)
    try:
        await page.screenshot(path=f"/app/tmpout/ts_{label}.png")
    except Exception:
        pass


async def main():
    from patchright.async_api import async_playwright
    pw = await async_playwright().start()
    b = await pw.chromium.launch(headless=True, args=dm.CHROMIUM_ARGS)
    ctx = await b.new_context(**dm.BROWSER_CONTEXT_KWARGS)
    await ctx.add_init_script(dm.STEALTH_INIT_SCRIPT)
    await probe("patchright", await ctx.new_page(), budget=60.0)
    await b.close()
    await pw.stop()

    from camoufox.async_api import AsyncCamoufox
    prof = dm._get_camoufox_profile(0)
    webgl = dm._get_camoufox_webgl_config(str(prof["os"]), prof["screen"])
    kw = dm._camoufox_launch_kwargs(prof, webgl, 0)
    async with AsyncCamoufox(**kw) as cb:
        await probe("camoufox", await cb.new_page(), budget=100.0)


asyncio.run(main())
