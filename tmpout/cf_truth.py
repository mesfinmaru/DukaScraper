"""Accurate Cloudflare challenge diagnosis.

`page.evaluate` in this Playwright build runs in a world that cannot see the
page's own globals (`window._cf_chl_opt` always read as undefined, which made an
earlier probe wrongly conclude the challenge engine never starts). Inline
scripts appended to the DOM *do* execute in the main world, so this probe
publishes values by writing them into a DOM node and reads that back.

Run: docker exec deep-worker python /app/tmpout/cf_truth.py [url] [os] [seconds]
"""

from __future__ import annotations

import asyncio
import json
import sys
import time

URL = sys.argv[1] if len(sys.argv) > 1 else "https://www.scrapingcourse.com/cloudflare-challenge"
_os = sys.argv[2] if len(sys.argv) > 2 else "auto"
PATIENCE = float(sys.argv[3]) if len(sys.argv) > 3 else 60.0

# Runs in the MAIN world (appended as a real <script>), publishes into the DOM.
MAIN_WORLD_JS = r"""
  var out = {};
  out.has_chl_opt = typeof window._cf_chl_opt !== 'undefined';
  try { out.chl_opt_keys = window._cf_chl_opt ? Object.keys(window._cf_chl_opt).slice(0, 12) : null; } catch (e) { out.chl_opt_keys = 'err'; }
  out.turnstile_loaded = typeof window.turnstile !== 'undefined';
  out.cf_chl_opt_cType = (window._cf_chl_opt && (window._cf_chl_opt.cType || window._cf_chl_opt.cRay)) || null;
  var el = document.getElementById('__probe_out');
  if (!el) { el = document.createElement('div'); el.id = '__probe_out'; el.style.display = 'none'; document.documentElement.appendChild(el); }
  el.textContent = JSON.stringify(out);
"""

DOM_READ_JS = "() => { const el = document.getElementById('__probe_out'); return el ? el.textContent : null; }"
#: Visible/interactive signals, readable from any world via the DOM.
SIGNALS_JS = r"""
() => ({
  title: document.title,
  url: location.href,
  bodyText: (document.body ? document.body.innerText : '').replace(/\s+/g, ' ').trim().slice(0, 160),
  turnstileWidget: !!document.querySelector('iframe[src*="challenges.cloudflare.com"], .cf-turnstile, #cf-turnstile, [data-sitekey]'),
  checkbox: !!document.querySelector('input[type="checkbox"]'),
  challengeStage: !!document.querySelector('#challenge-stage, #challenge-running, #cf-challenge-running'),
  scripts: document.querySelectorAll('script').length,
})
"""


async def main_world(page, js: str):
    """Execute `js` in the page's main world and return its published JSON.

    The Cloudflare page's CSP only allows inline scripts carrying its own nonce,
    so a bare appended <script> is blocked (which is what made an earlier probe
    report nothing). Borrow the nonce from a script already on the page; the CSS
    selector and the `.nonce` IDL property are readable from here, while the
    content attribute is hidden from `getAttribute`.
    """
    await page.evaluate(
        "code => { const src = document.querySelector('script[nonce]'); "
        "const s = document.createElement('script'); "
        "if (src && src.nonce) s.setAttribute('nonce', src.nonce); "
        "s.textContent = code; document.documentElement.appendChild(s); s.remove(); }",
        js,
    )
    await asyncio.sleep(0.4)
    raw = await page.evaluate(DOM_READ_JS)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return {"raw": raw}


async def main() -> None:
    from camoufox.async_api import AsyncCamoufox

    kw: dict[str, object] = {"headless": False, "humanize": True}
    if _os != "auto":
        kw["os"] = _os

    print(f"URL={URL} os={_os} patience={PATIENCE}s", flush=True)
    t0 = time.time()
    async with AsyncCamoufox(**kw) as browser:
        page = await browser.new_page()
        resp = await page.goto(URL, wait_until="domcontentloaded", timeout=60_000)
        print(f"goto status={resp.status if resp else None} cf-mitigated={resp.headers.get('cf-mitigated') if resp else None}", flush=True)

        start_url = page.url
        cleared = False
        while time.time() - t0 < PATIENCE:
            sig = await page.evaluate(SIGNALS_JS)
            mw = await main_world(page, MAIN_WORLD_JS)
            cookies = await page.context.cookies()
            cf = [c["name"] for c in cookies if "cf" in c["name"].lower()]
            on_challenge = bool(mw and mw.get("has_chl_opt")) or sig["challengeStage"] or "just a moment" in sig["title"].lower()
            print(
                f"t={time.time() - t0:5.1f}s chl_opt={mw and mw.get('has_chl_opt')} "
                f"turnstile={sig['turnstileWidget']} checkbox={sig['checkbox']} "
                f"cf_cookies={cf} title={sig['title']!r}",
                flush=True,
            )
            if not on_challenge:
                cleared = True
                print("CLEARED", flush=True)
                print("  body:", sig["bodyText"], flush=True)
                break
            if mw:
                print("     mw:", json.dumps(mw)[:220], flush=True)
            await asyncio.sleep(2.0)

        print(f"\ncleared={cleared} navigated_away={page.url != start_url} final={page.url}", flush=True)
        print("final title:", await page.title(), flush=True)


asyncio.run(main())
