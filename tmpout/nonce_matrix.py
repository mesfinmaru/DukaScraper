"""CSP variants; scripts signal success via document.title (DOM, world-independent)."""
import http.server

TPL = "<!doctype html><html><head><meta charset='utf-8'><title>NOT_RUN</title>{head}</head><body>{body}</body></html>"
INLINE_RUN = "<script>document.title='RAN';</script>"
INLINE_RUN_NONCE = "<script nonce=\"TESTNONCE\">document.title='RAN';</script>"

def page(head="", body=INLINE_RUN):
    return TPL.format(head=head, body=body).encode()

ROUTES = {
    "/nocsp":         (None, page()),
    "/nonce-header":  ("default-src 'none'; script-src 'nonce-TESTNONCE'", page(body=INLINE_RUN_NONCE)),
    "/nonce-meta":    (None, page(head="<meta http-equiv='Content-Security-Policy' content=\"default-src 'none'; script-src 'nonce-TESTNONCE'\">", body=INLINE_RUN_NONCE)),
    "/unsafe-inline": ("default-src 'none'; script-src 'unsafe-inline'", page()),
    "/ext-nonce":     ("default-src 'none'; script-src 'nonce-TESTNONCE'", page(body="<script src='/app.js' nonce='TESTNONCE'></script>")),
    "/plain-csp":     ("default-src 'none'; script-src 'self'", page()),
    "/app.js":        (None, b"document.title='RAN';"),
}

class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split("?")[0]
        if path not in ROUTES:
            self.send_response(404); self.end_headers(); return
        csp, body = ROUTES[path]
        self.send_response(200)
        self.send_header("Content-Type", "text/javascript" if path == "/app.js" else "text/html")
        if csp:
            self.send_header("Content-Security-Policy", csp)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass

http.server.HTTPServer(("127.0.0.1", 8793), H).serve_forever()
