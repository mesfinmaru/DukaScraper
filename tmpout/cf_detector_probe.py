"""Classify real vs challenge pages with the worker's live detector.

For each URL: title, _is_cloudflare_challenge verdict, Turnstile widget/token
state, strong interstitial markers in the raw HTML, visible-text markers, and
cf_clearance cookie presence. Proves whether the detector flags REAL pages
that merely embed a Turnstile widget.
"""
import asyncio, importlib.util, time

spec = importlib.util.spec_from_file_location("deepmain", "/app/workers/deep-worker/main.py")
dm = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dm)

URLS = [
    "https://www.scrapingcourse.com/login/cf-turnstile",
    "https://www.scrapingcourse.com/login/cf-antibot",
    "https://www.scrapingcourse.com/login",
    "https://www.scrapingcourse.com/cloudflare-challenge",
    "https://www.scrapingcourse.com/table-parsing",
]

WIDGET_JS = """() => ({
  widget: Boolean(document.querySelector('.cf-turnstile') ||
    document.querySelector('iframe[src*="challenges.cloudflare.com"]') ||
    document.querySelector('iframe[src*="turnstile"]') ||
    document.querySelector('input[name="cf-turnstile-response"]')),
  token: Array.from(document.querySelectorAll('input[name="cf-turnstile-response"]'))
    .some(i => Boolean(i.value && i.value.trim())),
})"""

async def classify(p, url, budget=45.0):
    t0 = time.time()
    await p.goto(url, wait_until="domcontentloaded", timeout=60000)
    # let an interstitial, if any, play out
    title = await p.title()
    while time.time() - t0 < budget and "moment" in title.lower():
        await asyncio.sleep(2)
        title = await p.title()
    is_chl = await dm._is_cloudflare_challenge(p)
    state = await p.evaluate(WIDGET_JS)
    html = await p.content()
    html_l = html.lower()
    strong = [m for m in dm._CF_CHALLENGE_STRONG_MARKERS if m.lower() in html_l]
    text = await p.evaluate("() => (document.body?.innerText||'').substring(0,1500)")
    loose = [m for m in dm._CF_CHALLENGE_MARKERS if m.lower() in (title + ' ' + (text or '')).lower()]
    names = [c["name"] for c in await p.context.cookies()]
    return is_chl, state, strong, loose, title, "cf_clearance" in names, time.time() - t0

async def main():
    from camoufox.async_api import AsyncCamoufox
    prof = dm._get_camoufox_profile(0)
    webgl = dm._get_camoufox_webgl_config(str(prof["os"]), prof["screen"])
    kw = dm._camoufox_launch_kwargs(prof, webgl, 0)
    async with AsyncCamoufox(**kw) as b:
        p = await b.new_page()
        for url in URLS:
            try:
                is_chl, state, strong, loose, title, clr, secs = await classify(p, url)
                print(f"{url.split('scrapingcourse.com')[1]:26s} is_challenge={is_chl!s:5s} "
                      f"widget={state['widget']} token={state['token']} strong={strong} "
                      f"loose_text={loose} clearance={clr} {secs:.1f}s\n"
                      f"    title={title[:64]!r}", flush=True)
            except Exception as e:
                print(f"{url}: ERROR {type(e).__name__}: {e}"[:200], flush=True)

asyncio.run(main())
