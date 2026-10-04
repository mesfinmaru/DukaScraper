import asyncio
URL = "http://host.docker.internal:8792/nocsp"
async def main():
    from camoufox.async_api import AsyncCamoufox
    async with AsyncCamoufox(headless=False, humanize=True) as b:
        p = await b.new_page()
        r = await p.goto(URL, wait_until="load", timeout=20000)
        print("status", r.status)
        print("html:", (await p.content())[:300])
        print("evaluate typeof window.__ran:", await p.evaluate("() => typeof window.__ran"))
        print("evaluate 1+1:", await p.evaluate("() => 1 + 1"))
        print("evaluate document.title:", await p.evaluate("() => document.title"))
        print("all globals containing ran:", await p.evaluate("() => Object.keys(window).filter(k => k.includes('ran'))"))
asyncio.run(main())
