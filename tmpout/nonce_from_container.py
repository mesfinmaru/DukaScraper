"""Load the validated host-side nonce page in container Camoufox and Chromium."""
import asyncio, sys

URL = "http://host.docker.internal:8791/"

async def camoufox():
    from camoufox.async_api import AsyncCamoufox
    msgs = []
    async with AsyncCamoufox(headless=False, humanize=True) as b:
        p = await b.new_page()
        p.on("console", lambda m: msgs.append(f"{m.type}: {m.text}"[:180]))
        await p.goto(URL, wait_until="load", timeout=30000)
        st = await p.evaluate("() => ({ran: window.__ran === true})")
        print("CAMOUFOX:", st)
        for m in msgs: print("   ", m)

async def chromium():
    from patchright.async_api import async_playwright
    msgs = []
    async with async_playwright() as pw:
        b = await pw.chromium.launch(headless=False)
        p = await b.new_page()
        p.on("console", lambda m: msgs.append(f"{m.type}: {m.text}"[:180]))
        await p.goto(URL, wait_until="load", timeout=30000)
        st = await p.evaluate("() => ({ran: window.__ran === true})")
        print("CHROMIUM:", st)
        for m in msgs: print("   ", m)
        await b.close()

print("target:", URL, flush=True)
asyncio.run(camoufox())
asyncio.run(chromium())
