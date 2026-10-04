import asyncio
URL = "http://host.docker.internal:8791/"
async def main():
    from camoufox.async_api import AsyncCamoufox
    async with AsyncCamoufox(headless=False, humanize=True) as b:
        p = await b.new_page()
        resp = await p.goto(URL, wait_until="load", timeout=30000)
        print("status:", resp.status)
        print("headers as browser sees them:")
        for k, v in resp.headers.items():
            print("  ", k, "=", v[:160])
        print("HTML:", (await p.content())[:600])
        print("script nonce property:", await p.evaluate(
            "() => { const s=[...document.querySelectorAll('script')][0]; return s ? {attr: s.getAttribute('nonce'), prop: s.nonce, text: s.textContent.slice(0,60)} : null; }"))
asyncio.run(main())
