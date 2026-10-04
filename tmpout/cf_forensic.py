"""Forensic Camoufox probe: why does the Cloudflare challenge never complete?

Captures what the plain probe cannot see - console output, uncaught page errors
and failed network requests from the interstitial - plus the Firefox-specific
signals (`navigator.oscpu`, `buildID`) that leak the real OS behind an OS spoof.

Run:  docker exec deep-worker python /app/tmpout/cf_forensic.py [url] [os] [locale]
"""

from __future__ import annotations

import asyncio
import json
import sys
import time

URL = sys.argv[1] if len(sys.argv) > 1 else "https://www.scrapingcourse.com/cloudflare-challenge"
_os_arg = sys.argv[2] if len(sys.argv) > 2 else "auto"
OS_SPOOF: str | None = None if _os_arg == "auto" else _os_arg
LOCALE = sys.argv[3] if len(sys.argv) > 3 else None

DEEP_JS = r"""
() => {
  const nav = navigator;
  const gl = (() => {
    try {
      const g = document.createElement('canvas').getContext('webgl');
      if (!g) return null;
      const d = g.getExtension('WEBGL_debug_renderer_info');
      return { vendor: d ? g.getParameter(d.UNMASKED_VENDOR_WEBGL) : null,
               renderer: d ? g.getParameter(d.UNMASKED_RENDERER_WEBGL) : null,
               version: g.getParameter(g.VERSION),
               maxTexture: g.getParameter(g.MAX_TEXTURE_SIZE) };
    } catch (e) { return {error: String(e)}; }
  })();
  return {
    oscpu: nav.oscpu,            // Firefox-only: reveals the REAL OS even when UA is spoofed
    buildID: nav.buildID,
    userAgent: nav.userAgent,
    platform: nav.platform,
    appVersion: nav.appVersion,
    productSub: nav.productSub,
    vendor: nav.vendor,
    languages: nav.languages,
    language: nav.language,
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    tzOffset: new Date().getTimezoneOffset(),
    userAgentData: nav.userAgentData ? 'present' : 'absent',
    screen: [screen.width, screen.height, screen.colorDepth, devicePixelRatio],
    webgl: gl,
    fonts: (() => {
      // Real-OS font probes: a Windows/Linux box claiming macOS answers oddly here.
      const probe = ['Apple Color Emoji','Segoe UI','Helvetica Neue','DejaVu Sans','Liberation Sans','Arial'];
      const c = document.createElement('canvas').getContext('2d');
      const base = '72px monospace';
      const w = f => { c.font = base; const a = c.measureText('mmmmmmmmmmlli').width;
                       c.font = '72px "' + f + '", monospace'; return c.measureText('mmmmmmmmmmlli').width !== a; };
      return probe.filter(w);
    })(),
  };
}
"""


async def main() -> None:
    from camoufox.async_api import AsyncCamoufox

    _kw: dict[str, object] = {"headless": False, "humanize": True}
    if OS_SPOOF:
        _kw["os"] = OS_SPOOF
    if LOCALE:
        _kw["locale"] = LOCALE

    console: list[str] = []
    errors: list[str] = []
    failed: list[str] = []

    print(f"URL={URL} os_spoof={OS_SPOOF or 'auto'} locale={LOCALE or 'auto'}", flush=True)
    t0 = time.time()
    async with AsyncCamoufox(**_kw) as browser:
        page = await browser.new_page()
        page.on("console", lambda m: console.append(f"{m.type}: {m.text}"[:300]))
        page.on("pageerror", lambda e: errors.append(str(e)[:300]))
        page.on("requestfailed", lambda r: failed.append(f"{r.url[:110]} :: {r.failure}"))

        resp = await page.goto(URL, wait_until="domcontentloaded", timeout=60_000)
        print(f"goto -> status={resp.status if resp else None} headers={dict(resp.headers) if resp else {}}", flush=True)

        print("DEEP:", json.dumps(await page.evaluate(DEEP_JS), indent=2), flush=True)

        await asyncio.sleep(12)

        # What does the interstitial actually load, and did it error?
        print("\n--- requests seen ---", flush=True)
        try:
            html = await page.content()
            import re
            srcs = re.findall(r'<script[^>]+src="([^"]+)"', html)
            print("script srcs:", srcs[:8], flush=True)
            print("has cf-chl script:", "cf-chl" in html or "challenge-platform" in html, flush=True)
            print("(script inline len):", len(html), flush=True)
        except Exception as exc:
            print("content error:", exc, flush=True)

        print("\n--- console ---", flush=True)
        for c in console[:25]:
            print("  ", c, flush=True)
        print("\n--- page errors ---", flush=True)
        for e in errors[:15]:
            print("  ", e, flush=True)
        print("\n--- failed requests ---", flush=True)
        for f in failed[:15]:
            print("  ", f, flush=True)

        print(f"\nelapsed={time.time() - t0:.1f}s title={await page.title()!r}", flush=True)


asyncio.run(main())
