"""Does Camoufox break nonce-protected inline scripts, independent of Cloudflare?

Serves a local page whose inline script carries a nonce matching a
`script-src 'nonce-...'` CSP, then loads it in headful Camoufox and reports
whether the script ran. A local page removes Cloudflare, the network and the
site from the picture: if the script is blocked here, Camoufox's launch is
what breaks CSP nonces.
"""

from __future__ import annotations

import asyncio
import http.server
import socket
import threading

PAGE = """<!doctype html><html><head><meta charset="utf-8">
<title>nonce control</title>
</head><body>
<script nonce="TESTNONCE">
  window.__ran = true;
  window.__probe = { started: true };
</script>
<script src="/external.js" nonce="TESTNONCE"></script>
</body></html>"""

EXTERNAL = "window.__externalRan = true;"

CSP = "default-src 'none'; script-src 'nonce-TESTNONCE'; style-src 'unsafe-inline'"


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        body = (EXTERNAL if self.path.startswith("/external") else PAGE).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/javascript" if self.path.startswith("/external") else "text/html")
        self.send_header("Content-Security-Policy", CSP)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # silence
        pass


def _serve() -> tuple[str, http.server.HTTPServer]:
    srv = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return f"http://127.0.0.1:{srv.server_address[1]}/", srv


async def main() -> None:
    from camoufox.async_api import AsyncCamoufox

    url, srv = _serve()
    print(f"control page at {url} with CSP: {CSP}", flush=True)

    console: list[str] = []
    try:
        async with AsyncCamoufox(headless=False, humanize=True) as browser:
            page = await browser.new_page()
            page.on("console", lambda m: console.append(f"{m.type}: {m.text}"[:240]))
            await page.goto(url, wait_until="load", timeout=30_000)
            state = await page.evaluate(
                "() => ({ ran: window.__ran === true, externalRan: window.__externalRan === true })"
            )
            print("RESULT:", state, flush=True)
            print("console:", flush=True)
            for c in console:
                print("   ", c, flush=True)
            if not state["ran"]:
                print("\n=> Camoufox BLOCKED a valid nonce-protected inline script.", flush=True)
            else:
                print("\n=> nonce-protected inline scripts work; CSP is not the cause.", flush=True)
    finally:
        srv.shutdown()


asyncio.run(main())
