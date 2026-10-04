"""Does the timezone need to match the egress IP, or is any explicit value enough?"""
import asyncio, json, sys, time

URL = "https://www.scrapingcourse.com/cloudflare-challenge"
PROBE = r"""
var el = document.getElementById('__p');
if (!el) { el = document.createElement('div'); el.id='__p'; el.style.display='none'; document.documentElement.appendChild(el); }
el.textContent = JSON.stringify({tz: Intl.DateTimeFormat().resolvedOptions().timeZone, offset: new Date().getTimezoneOffset()});
"""

async def run(label, patience=70.0, **kw):
    from camoufox.async_api import AsyncCamoufox
    from camoufox.utils import Screen
    t0 = time.time(); tz = "?"
    try:
        async with AsyncCamoufox(headless=False, humanize=True, screen=Screen(max_width=1920, max_height=1080),
                                 window=(1920, 1040), os="windows", locale="en-US", **kw) as b:
            p = await b.new_page()
            await p.goto(URL, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(3)
            for _ in range(6):
                await p.evaluate("code => { const src=document.querySelector('script[nonce]'); const s=document.createElement('script'); if(src&&src.nonce) s.setAttribute('nonce', src.nonce); s.textContent=code; document.documentElement.appendChild(s); s.remove(); }", PROBE)
                await asyncio.sleep(0.5)
                raw = await p.evaluate("() => { const e=document.getElementById('__p'); return e ? e.textContent : null; }")
                if raw:
                    tz = json.loads(raw)["tz"]; break
                await asyncio.sleep(1)
            while time.time() - t0 < patience:
                if "moment" not in (await p.title()).lower():
                    print(f"  {label:18s} PASS {time.time()-t0:5.1f}s spoofed_tz={tz}", flush=True); return True
                await asyncio.sleep(2)
            print(f"  {label:18s} fail {time.time()-t0:5.1f}s spoofed_tz={tz}", flush=True); return False
    except Exception as e:
        print(f"  {label:18s} ERROR {str(e)[:110]}", flush=True); return False

async def main():
    for name in sys.argv[1].split(","):
        if name == "geoip":
            await run("geoip", geoip=True)
        elif name == "ny":
            await run("tz=America/New_York", config={"timezone": "America/New_York"})
        elif name == "nairobi":
            await run("tz=Africa/Nairobi", config={"timezone": "Africa/Nairobi"})

asyncio.run(main())
