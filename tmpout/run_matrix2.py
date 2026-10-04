import asyncio
BASE = "http://host.docker.internal:8793"
PATHS = ["/nocsp", "/nonce-header", "/nonce-meta", "/unsafe-inline", "/ext-nonce", "/plain-csp"]

async def drive(page, label):
    for p in PATHS:
        await page.goto(BASE + p, wait_until="load", timeout=20000)
        print(f"  {label:9s} {p:15s} title={await page.title()!r}")

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
