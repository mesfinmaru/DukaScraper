"""Live Camoufox diagnostic against a Cloudflare challenge, run inside deep-worker.

Answers, empirically:
  * is the browser truly HEADFUL (a real X window on :99)?
  * does the display/GL stack look plausible to Cloudflare?
  * which JS-visible signals are inconsistent (webdriver, screen, WebGL, timezone)?
  * can it clear the challenge at all, and how long does it take?

Run:  docker exec deep-worker python /app/tmpout/cf_probe.py [url]
"""

from __future__ import annotations

import asyncio
import json
import sys
import time

URL = sys.argv[1] if len(sys.argv) > 1 else "https://www.scrapingcourse.com/cloudflare-challenge"
#: OS to spoof. "auto" leaves Camoufox to detect the container's real OS
#: (linux), which is what the experiment varies.
_os_arg = sys.argv[2] if len(sys.argv) > 2 else "auto"
OS_SPOOF: str | None = None if _os_arg == "auto" else _os_arg
LOCALE = sys.argv[3] if len(sys.argv) > 3 else None

FINGERPRINT_JS = r"""
() => {
  const gl = (() => {
    try {
      const c = document.createElement('canvas');
      const g = c.getContext('webgl') || c.getContext('experimental-webgl');
      if (!g) return null;
      const dbg = g.getExtension('WEBGL_debug_renderer_info');
      return {
        vendor: dbg ? g.getParameter(dbg.UNMASKED_VENDOR_WEBGL) : g.getParameter(g.VENDOR),
        renderer: dbg ? g.getParameter(dbg.UNMASKED_RENDERER_WEBGL) : g.getParameter(g.RENDERER),
      };
    } catch (e) { return { error: String(e) }; }
  })();
  const nav = navigator;
  return {
    webdriver: nav.webdriver,
    userAgent: nav.userAgent,
    platform: nav.platform,
    languages: nav.languages,
    hardwareConcurrency: nav.hardwareConcurrency,
    deviceMemory: nav.deviceMemory,
    maxTouchPoints: nav.maxTouchPoints,
    plugins: nav.plugins ? nav.plugins.length : null,
    screen: {
      width: screen.width, height: screen.height,
      availWidth: screen.availWidth, availHeight: screen.availHeight,
      colorDepth: screen.colorDepth, pixelDepth: screen.pixelDepth,
    },
    window: { outerWidth: outerWidth, outerHeight: outerHeight, innerWidth: innerWidth, innerHeight: innerHeight },
    devicePixelRatio,
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    offset: new Date().getTimezoneOffset(),
    webgl: gl,
    hasChrome: typeof window.chrome !== 'undefined',
    permissions: typeof navigator.permissions !== 'undefined',
    title: document.title,
  };
}
"""

CHALLENGE_JS = r"""
() => {
  const t = document.title || '';
  const body = (document.body && document.body.innerText || '').slice(0, 400);
  return {
    title: t,
    isChallenge: /just a moment|checking your browser|attention required|verify you are human|cf-|cloudflare/i.test(t),
    bodyPreview: body.replace(/\s+/g, ' ').trim().slice(0, 220),
    iframes: [...document.querySelectorAll('iframe')].map(f => f.src || '(srcless)').slice(0, 6),
    hasTurnstile: !!document.querySelector('#challenge-stage, .cf-turnstile, input[type="checkbox"]'),
    inputs: [...document.querySelectorAll('input')].map(i => i.type).slice(0, 6),
    readyState: document.readyState,
  };
}
"""


async def main() -> None:
    from camoufox.async_api import AsyncCamoufox

    print(f"DISPLAY={__import__('os').environ.get('DISPLAY')}")
    print(f"URL={URL}")
    print(f"os_spoof={OS_SPOOF or 'auto (container default)'} locale={LOCALE or 'auto'}")
    print("launching headful Camoufox …", flush=True)

    _launch_kwargs: dict[str, object] = {"headless": False, "humanize": True}
    if OS_SPOOF:
        _launch_kwargs["os"] = OS_SPOOF
    if LOCALE:
        _launch_kwargs["locale"] = LOCALE

    t0 = time.time()
    async with AsyncCamoufox(**_launch_kwargs) as browser:
        page = await browser.new_page()
        print(f"launched in {time.time() - t0:.1f}s", flush=True)

        # Is this genuinely a headful browser painting to :99?
        win = await page.evaluate("() => ({outer: [outerWidth, outerHeight], inner: [innerWidth, innerHeight]})")
        print("viewport:", json.dumps(win), flush=True)

        resp = await page.goto(URL, wait_until="domcontentloaded", timeout=60_000)
        print(f"goto -> {resp.status if resp else None}", flush=True)

        fp = await page.evaluate(FINGERPRINT_JS)
        print("FINGERPRINT:", json.dumps(fp, indent=2), flush=True)

        for i in range(14):
            state = await page.evaluate(CHALLENGE_JS)
            cookies = await page.context.cookies()
            cf = [c["name"] for c in cookies if "cf" in c["name"].lower()]
            print(
                f"[{i:02d} t={time.time() - t0:5.1f}s] title={state['title']!r} "
                f"challenge={state['isChallenge']} turnstile={state['hasTurnstile']} "
                f"cf_cookies={cf}",
                flush=True,
            )
            if not state["isChallenge"]:
                print("CLEARED.", flush=True)
                print("body:", state["bodyPreview"], flush=True)
                break
            if i == 2:
                print("  state:", json.dumps(state, indent=2), flush=True)
            await asyncio.sleep(3)

        final = await page.evaluate(CHALLENGE_JS)
        print("FINAL:", json.dumps(final, indent=2), flush=True)
        print(f"elapsed={time.time() - t0:.1f}s", flush=True)


asyncio.run(main())
