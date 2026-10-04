"""Forensic probe: why does /login/cf-turnstile never yield a token?

Uses the production Patchright context (same CHROMIUM_ARGS / context kwargs /
stealth script as the deep worker) and:
  1. dumps the widget DOM, iframes, boxes, turnstile config
  2. polls the token for 25s with NO interaction (managed/invisible mode check)
  3. then tries click strategies and re-polls
  4. records console errors and failed sub-requests
"""
import asyncio
import importlib.util
import json
import time

URL = "https://www.scrapingcourse.com/login/cf-turnstile"

spec = importlib.util.spec_from_file_location("deepmain", "/app/workers/deep-worker/main.py")
dm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dm)


DUMP_JS = """() => {
  const out = {};
  const turnstileDivs = Array.from(document.querySelectorAll('.cf-turnstile'));
  out.divs = turnstileDivs.map(d => ({
      cls: d.className,
      attrs: Array.from(d.attributes).map(a => a.name + '=' + a.value.slice(0, 80)),
      rect: (r => ({x: Math.round(r.x), y: Math.round(r.y), w: Math.round(r.width), h: Math.round(r.height)}))(d.getBoundingClientRect()),
      childCount: d.children.length,
      html: d.outerHTML.slice(0, 500),
  }));
  out.iframes = Array.from(document.querySelectorAll('iframe')).map(f => ({
      src: (f.src || '').slice(0, 120),
      title: f.title || '',
      id: f.id || '',
      rect: (r => ({x: Math.round(r.x), y: Math.round(r.y), w: Math.round(r.width), h: Math.round(r.height)}))(f.getBoundingClientRect()),
      visible: !!(r0 => r0.width && r0.height)(f.getBoundingClientRect()),
  }));
  const resp = Array.from(document.querySelectorAll('input[name="cf-turnstile-response"]'));
  out.responseInputs = resp.map(i => ({len: (i.value || '').length, head: (i.value || '').slice(0, 24), type: i.type, id: i.id, name: i.name}));
  out.api = typeof window.turnstile;
  out.text = (document.body ? document.body.innerText : '').replace(/\\s+/g, ' ').slice(0, 400);
  return out;
}"""

TOKEN_JS = """() => Array.from(
    document.querySelectorAll('input[name="cf-turnstile-response"]')
  ).map(i => (i.value || '').trim()).filter(v => v.length > 0)"""


async def token_now(page):
    try:
        vals = await page.evaluate(TOKEN_JS)
    except Exception as exc:
        return None, f"ERR {type(exc).__name__}"
    if not vals:
        return False, ""
    return True, f"len={len(vals[0])} head={vals[0][:20]}"


async def main():
    from patchright.async_api import async_playwright

    console, failed = [], []
    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=True, args=dm.CHROMIUM_ARGS)
    ctx = await browser.new_context(**dm.BROWSER_CONTEXT_KWARGS)
    await ctx.add_init_script(dm.STEALTH_INIT_SCRIPT)
    page = await ctx.new_page()
    page.on("console", lambda m: console.append(f"{m.type}: {m.text}"[:240]))
    page.on("requestfailed",
            lambda r: failed.append(f"{r.url[:110]} :: {r.failure}"))
    page.on("response",
            lambda r: failed.append(f"HTTP {r.status} {r.url[:110]}")
            if ("challenges.cloudflare.com" in r.url or "/cdn-cgi/" in r.url) else None)

    t0 = time.time()
    r = await page.goto(URL, wait_until="domcontentloaded", timeout=60000)
    print(f"nav status={r.status if r else None} in {time.time()-t0:.1f}s", flush=True)
    await asyncio.sleep(4)

    try:
        dump = await page.evaluate(DUMP_JS)
    except Exception as exc:
        dump = {"error": f"{type(exc).__name__}: {exc}"}
    print("\n=== DUMP @4s ===")
    print(json.dumps(dump, indent=1)[:4000], flush=True)

    # ---- phase 1: no interaction at all (managed / invisible mode?) ----
    print("\n=== PHASE 1: passive poll 25s (no clicks) ===", flush=True)
    for i in range(25):
        ok, detail = await token_now(page)
        print(f"  t+{i:>2}s token={ok} {detail}", flush=True)
        if ok:
            break
        await asyncio.sleep(1.0)

    # ---- phase 2: click strategies ----
    print("\n=== PHASE 2: click strategies ===", flush=True)
    clicked = await dm._click_turnstile_widget(page)
    print(f"  _click_turnstile_widget -> {clicked}", flush=True)
    for i in range(20):
        await asyncio.sleep(1.0)
        ok, detail = await token_now(page)
        print(f"  t+{i:>2}s after click token={ok} {detail}", flush=True)
        if ok:
            break

    # ---- phase 3: what does the iframe actually contain? ----
    print("\n=== PHASE 3: iframe internals ===", flush=True)
    for sel in ("iframe[src*='challenges.cloudflare.com']", "iframe[title*='Cloudflare']"):
        el = await page.query_selector(sel)
        if not el:
            print(f"  {sel}: NOT FOUND", flush=True)
            continue
        box = await el.bounding_box()
        print(f"  {sel}: box={box}", flush=True)
        try:
            frame = await el.content_frame()
            if frame is None:
                print("    content_frame=None (cross-origin opaque)", flush=True)
                continue
            inner = await frame.evaluate("() => document.body ? document.body.innerHTML.slice(0, 800) : 'NO BODY'")
            txt = await frame.evaluate("() => (document.body ? document.body.innerText : '').replace(/\\s+/g,' ').slice(0,200)")
            print(f"    innerHTML: {inner[:500]}", flush=True)
            print(f"    innerText: {txt!r}", flush=True)
        except Exception as exc:
            print(f"    frame read failed: {type(exc).__name__}: {exc}", flush=True)

    print("\n=== CONSOLE (last 25) ===", flush=True)
    for c in console[-25:]:
        print("  ", c, flush=True)
    print("\n=== CF REQUESTS / FAILURES ===", flush=True)
    for c in failed[:40]:
        print("  ", c, flush=True)

    await browser.close()
    await pw.stop()


asyncio.run(main())
