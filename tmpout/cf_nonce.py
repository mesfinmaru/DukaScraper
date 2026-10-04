"""Compare the CSP header nonce with the nonce on the page's inline scripts.

If Cloudflare sends `script-src 'nonce-XYZ'` but the inline challenge script
carries no nonce (or a different one), the browser blocks it, the challenge
engine never starts, and the interstitial loops forever.
"""

from __future__ import annotations

import asyncio
import re
import sys
import time

URL = sys.argv[1] if len(sys.argv) > 1 else "https://www.scrapingcourse.com/cloudflare-challenge"


async def main() -> None:
    from camoufox.async_api import AsyncCamoufox

    console: list[str] = []
    async with AsyncCamoufox(headless=False, humanize=True) as browser:
        page = await browser.new_page()
        page.on("console", lambda m: console.append(f"{m.type}: {m.text}"[:260]))

        resp = await page.goto(URL, wait_until="domcontentloaded", timeout=60_000)
        csp = (resp.headers.get("content-security-policy") if resp else "") or ""
        header_nonces = re.findall(r"'nonce-([^']+)'", csp)
        print("CSP header nonces:", header_nonces, flush=True)
        print("cf-mitigated:", resp.headers.get("cf-mitigated") if resp else None, flush=True)

        dom = await page.evaluate(r"""
        () => {
          const scripts = [...document.querySelectorAll('script')];
          return scripts.map((s, i) => ({
            i,
            src: s.src || null,
            nonceAttr: s.getAttribute('nonce'),
            nonceProp: s.nonce,
            inline: !s.src,
            len: (s.textContent || '').length,
          })).filter(s => s.inline || s.nonceAttr || s.nonceProp);
        }
        """)
        print("\nDOM scripts:", flush=True)
        for s in dom:
            print("  ", s, flush=True)

        # Which nonces actually reached the DOM?
        dom_nonces = {s["nonceAttr"] for s in dom if s["nonceAttr"]} | {
            s["nonceProp"] for s in dom if s["nonceProp"]
        }
        print("\nDOM nonces:", dom_nonces, flush=True)
        print("header/DOM match:", bool(dom_nonces & set(header_nonces)), flush=True)

        await asyncio.sleep(8)
        print("\nCSP violations from console:", flush=True)
        for c in console:
            if "Content-Security-Policy" in c or "blocked" in c:
                print("  ", c, flush=True)

        # Is the challenge script present but inert?
        inert = await page.evaluate(
            "() => ({ orbitals: typeof window._cf_chl_opt, t: window._cf_chl_opt && Object.keys(window._cf_chl_opt).length })"
        )
        print("\nchallenge engine state:", inert, flush=True)
        print("title:", await page.title(), flush=True)


asyncio.run(main())
