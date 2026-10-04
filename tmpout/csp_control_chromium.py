"""Same nonce control page, loaded in Patchright Chromium (control-of-the-control)."""
import asyncio, http.server, sys, threading
sys.path.insert(0, "/app/tmpout")
from csp_control import Handler, PAGE  # reuse the exact page + CSP

async def main():
    from patchright.async_api import async_playwright
    srv = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{srv.server_address[1]}/"
    try:
        async with async_playwright() as p:
            b = await p.chromium.launch(headless=False)
            page = await b.new_page()
            msg = []
            page.on("console", lambda m: msg.append(f"{m.type}: {m.text}"[:200]))
            await page.goto(url, wait_until="load", timeout=30000)
            state = await page.evaluate("() => ({ran: window.__ran===true, externalRan: window.__externalRan===true})")
            print("CHROMIUM RESULT:", state)
            for m in msg: print("  ", m)
            await b.close()
    finally:
        srv.shutdown()

asyncio.run(main())
