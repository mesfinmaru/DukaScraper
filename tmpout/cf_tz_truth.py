"""Read the MAIN-world (spoofed) timezone/offset, then measure pass/fail."""
import asyncio, json, sys, time

URL = "https://www.scrapingcourse.com/cloudflare-challenge"
PROBE = r"""
var el = document.getElementById('__p');
if (!el) { el = document.createElement('div'); el.id='__p'; el.style.display='none'; document.documentElement.appendChild(el); }
el.textContent = JSON.stringify({
  tz: Intl.DateTimeFormat().resolvedOptions().timeZone,
  offset: new Date().getTimezoneOffset(),
  lang: navigator.language,
  hc: navigator.hardwareConcurrency,
  ua: navigator.userAgent.slice(0, 70),
});
"""

async def run(label, patience=40.0, **kw):
    from camoufox.async_api import AsyncCamoufox
    from camoufox.utils import Screen
    t0 = time.time()
    async with AsyncCamoufox(headless=False, humanize=True, screen=Screen(max_width=1920, max_height=1080),
                             window=(1920, 1040), os="windows", locale="en-US", **kw) as b:
        p = await b.new_page()
        await p.goto(URL, wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(3)
        res = None
        for _ in range(6):
            await p.evaluate("code => { const src=document.querySelector('script[nonce]'); const s=document.createElement('script'); if(src&&src.nonce) s.setAttribute('nonce', src.nonce); s.textContent=code; document.documentElement.appendChild(s); s.remove(); }", PROBE)
            await asyncio.sleep(0.5)
            raw = await p.evaluate("() => { const e=document.getElementById('__p'); return e ? e.textContent : null; }")
            if raw:
                res = json.loads(raw); break
            await asyncio.sleep(1)
        print(f"[{label}] main-world fingerprint: {res}", flush=True)
        while time.time() - t0 < patience:
            t = await p.title()
            if "moment" not in t.lower():
                print(f"[{label}] PASS in {time.time()-t0:.1f}s", flush=True)
                return True
            await asyncio.sleep(2)
        print(f"[{label}] FAIL after {patience:.0f}s (isolated-world tz={await p.evaluate('() => Intl.DateTimeFormat().resolvedOptions().timeZone')})", flush=True)
        return False

async def main():
    which = sys.argv[1]
    if which in ("base", "both"):
        await run("base", config=None)
    if which in ("tz", "both"):
        await run("tz", config={"timezone": "Africa/Nairobi"})

asyncio.run(main())
