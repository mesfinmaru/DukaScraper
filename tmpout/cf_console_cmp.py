import asyncio, re, sys
URL = "https://www.scrapingcourse.com/cloudflare-challenge"

async def run(label, page, browser_kind):
    console = []
    page.on("console", lambda m: console.append(f"{m.type}: {m.text}"[:230]))
    r = await page.goto(URL, wait_until="domcontentloaded", timeout=60000)
    await asyncio.sleep(12)
    sig = await page.evaluate("""() => ({
        title: document.title,
        widgets: document.querySelectorAll('iframe[src*="challenges.cloudflare.com"], .cf-turnstile').length,
        csp_errs: 0,
    })""")
    print(f"\n=== {label} ===")
    print("status:", r.status if r else None, "sig:", sig)
    csp = [c for c in console if "Content-Security-Policy" in c]
    print("CSP violations:", len(csp))
    for c in csp[:4]: print("   ", c)
    print("other console (first 6):")
    for c in [c for c in console if "Content-Security-Policy" not in c][:6]:
        print("   ", c)

async def main():
    from camoufox.async_api import AsyncCamoufox
    async with AsyncCamoufox(headless=False, humanize=True) as b:
        await run("CAMOUFOX", await b.new_page(), "firefox")
    from patchright.async_api import async_playwright
    async with async_playwright() as pw:
        b = await pw.chromium.launch(headless=False)
        await run("PATCHRIGHT CHROMIUM", await b.new_page(), "chromium")
        await b.close()

asyncio.run(main())
