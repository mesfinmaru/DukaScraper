import asyncio

async def probe(label, browser):
    p = await browser.new_page()
    # A) set_content: inline script the browser parses directly
    await p.set_content("<html><body><script>window.__ran=1;document.title='RAN';</script></body></html>")
    print(f"{label} set_content  -> ran={await p.evaluate('() => window.__ran === 1')} title={await p.title()!r}")

    # B) about:blank + JS-inserted script
    await p.goto("about:blank")
    await p.evaluate("() => { const s=document.createElement('script'); s.textContent='window.__ran2=1'; document.body.appendChild(s); }")
    print(f"{label} inserted js   -> ran2={await p.evaluate('() => window.__ran2 === 1')}")

    # C) real site over https
    try:
        await p.goto("https://example.com", wait_until="load", timeout=25000)
        await p.evaluate("() => { const s=document.createElement('script'); s.textContent='window.__ran3=1'; document.body.appendChild(s); }")
        print(f"{label} example.com   -> ran3={await p.evaluate('() => window.__ran3 === 1')} title={await p.title()!r}")
    except Exception as e:
        print(f"{label} example.com   -> ERR {str(e)[:70]}")
    await p.close()

async def main():
    from camoufox.async_api import AsyncCamoufox
    async with AsyncCamoufox(headless=False, humanize=True) as b:
        await probe("camoufox ", b)
    from patchright.async_api import async_playwright
    async with async_playwright() as pw:
        b = await pw.chromium.launch(headless=False)
        await probe("chromium ", b)
        await b.close()

asyncio.run(main())
