"""Serve the nonce control page on the host so a real browser can validate it."""
import http.server

PAGE = """<!doctype html><html><head><meta charset="utf-8"><title>nonce control</title>
<script nonce="TESTNONCE">window.__ran = true;</script>
</head><body>control page</body></html>"""

CSP = "default-src 'none'; script-src 'nonce-TESTNONCE'; style-src 'unsafe-inline'"

class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = PAGE.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Security-Policy", CSP)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass

http.server.HTTPServer(("127.0.0.1", 8791), H).serve_forever()
