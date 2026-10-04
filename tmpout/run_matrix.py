import asyncio

BASE = "http://host.docker.internal:8792"
PATHS = ["/nocsp", "/nonce-header", "/nonce-meta", "/unsafe-inline", "/ext-nonce"]

async def drive(page, label):
    for p in PATHS:
        msgs = []
        page.on("console", lambda m: None)
        try:
            await page.goto(BASE + p, wait_until="load", timeout=20000)
            st = await page.evaluate("() => window.__ran === 1")
        except Exception as e:
            st = f"ERR {e}"[:60]
        print(f"  {label:9s} {p:15s} ran={st}")

async def main():
    from camoufox.async_api import AsyncCamoufox
    async with AsyncCamoufox(headless=False, humanize=True) as b:
        print("CAMOUFOX")
        await drive(await b.new_page(), "camoufox")

    from patchright.async_api import async_playwright
    async with async_playwright() as pw:
        b = await pw.chromium.launch(headless=False)
        print("PATCHRIGHT CHROMIUM")
        await drive(await b.new_page(), "chromium")
        await b.close()

asyncio.run(main())
