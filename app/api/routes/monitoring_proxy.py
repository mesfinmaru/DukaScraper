"""Embedding proxy for Grafana and Prometheus.

Grafana 13 sends ``X-Frame-Options: deny`` on every response and no longer
honours the ``allow_embedding`` ini setting (``GF_ALLOW_EMBEDDING`` is still
accepted, but it no longer removes the header). The browser therefore refuses to
frame the Monitoring page's dashboard with ``ERR_BLOCKED_BY_RESPONSE``: the
iframe loads, then renders nothing. There is no Grafana-side knob left to turn,
so the header is stripped here.

Why this lives in the API rather than an nginx sidecar:

* Grafana's embedding behaviour has now broken once already; routing through
  code we own means the next change is a one-line edit here instead of a
  debugging session across three containers.
* Grafana runs with anonymous Viewer access already, so no Grafana credentials
  are handled or logged by this path.
* The proxy inherits the API's admin check.

Mount layout and why it is at the app root
------------------------------------------
Each tool is mounted at ``/grafana`` and ``/prometheus``, not nested under
``/api/v1/monitoring/grafana``. Grafana builds every URL it generates (assets,
API calls, websocket endpoints) from its own ``root_url``; with
``GF_SERVER_SERVE_FROM_SUB_PATH=true`` and a matching ``GF_SERVER_ROOT_URL`` it
emits correctly-prefixed URLs by itself. Behind a deeper prefix it does not, and
every route it builds 404s - which is exactly the "Page not found" this module
originally produced. Mounting at the root plus the two Grafana settings removes
the need to rewrite HTML at all.

Authentication and framing pull in opposite directions
-----------------------------------------------------
An ``<iframe>`` cannot send an ``Authorization`` header, and the top-level
document's query string is not inherited by the tool's own sub-resource
requests (its multi-megabyte JS/CSS bundles). So the UI exchanges its bearer
token for a short-lived embed token (``GET /monitoring/embed-token``), which is
returned both in the body and as an HttpOnly cookie. The body value goes in the
iframe ``src``; the cookie is what authenticates everything the document then
pulls in.
"""

from __future__ import annotations

import hashlib
import hmac
import time

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import Response
from fastapi.security import HTTPAuthorizationCredentials

from app.common.config.settings import settings
from app.common.logger.logger import logger
from app.security.auth import bearer_scheme, get_current_user, require_admin

#: Mount prefix -> (in-network upstream, sub-path the tool expects on requests).
#: Not the browser-facing URLs in settings: ``localhost:3000`` from inside a
#: container resolves to the host, not Grafana.
TOOLS = {
    "grafana": ("http://grafana:3000", "/grafana"),
    "prometheus": ("http://prometheus:9090", "/prometheus"),
}

#: How long an embed token stays valid. Long enough for a dashboard to load and
#: keep refreshing, short enough that a leaked URL (browser history, a pasted
#: screenshot) is worthless within minutes.
EMBED_TOKEN_TTL_SECONDS = 900

#: Upstream timeout. Grafana's first dashboard load compiles and fetches a lot;
#: the default 5s would abort it and leave a blank frame.
UPSTREAM_TIMEOUT_SECONDS = 30.0

#: Cookie carrying the embed token to the mounted tool apps.
EMBED_COOKIE_NAME = "duka_embed"

#: Hop-by-hop headers that must not be forwarded. `Connection`-listed headers
#: belong to a single transport hop and are meaningless (and in the case of
#: `Transfer-Encoding`, actively harmful) to copy onto a new one.
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}

#: Never echoed to the browser regardless of what upstream sent.
_STRIPPED_RESPONSE_HEADERS = {
    "x-frame-options",
    "content-security-policy",
    "content-security-policy-report-only",
}


def _embed_secret() -> bytes:
    """Key for signing embed tokens, derived from the API's JWT secret.

    Domain-separated so an embed token can never be replayed as an access token
    (or vice versa) even though both are signed with the same underlying secret.
    """
    return hashlib.sha256(f"monitoring-embed:{settings.JWT_SECRET_KEY}".encode()).digest()


def issue_embed_token() -> str:
    """Mint a time-limited token that authorises the embed proxy."""
    expires_at = int(time.time()) + EMBED_TOKEN_TTL_SECONDS
    payload = f"{expires_at}"
    signature = hmac.new(_embed_secret(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}.{signature}"


def verify_embed_token(token: str | None) -> bool:
    """Constant-time check of an embed token's signature and expiry."""
    if not token or "." not in token:
        return False
    expires_raw, _, signature = token.partition(".")
    try:
        expires_at = int(expires_raw)
    except ValueError:
        return False
    expected = hmac.new(_embed_secret(), expires_raw.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        return False
    return time.time() < expires_at


# ---------------------------------------------------------------------------
# /monitoring routes (kept under the API prefix; they are not proxied content)
# ---------------------------------------------------------------------------

router = APIRouter()


@router.get("/embed-token")
async def get_embed_token(response: Response, user: dict = Depends(require_admin)):
    """Exchange the caller's bearer token for a short-lived embed token.

    Admin-only: this is what makes the iframe work, and the iframe cannot present
    a bearer token of its own.

    The token is *also* set as a cookie, and that is not belt-and-braces - it is
    the part that actually makes the dashboard usable. Grafana's page pulls
    several megabytes of JS/CSS/images from sub-paths, and none of those requests
    inherit the query string of the top-level document, so with query-token auth
    alone every asset 401s and Grafana renders its "failed to load its
    application files" page. A cookie is attached by the browser to every
    subsequent request, sub-resources included.

    ``path="/"`` because the two tools are mounted at ``/grafana`` and
    ``/prometheus``; a single narrower path could not cover both. The cookie is
    HttpOnly and only ever read by the proxy handlers, so the wider scope grants
    nothing.
    """
    token = issue_embed_token()
    response.set_cookie(
        key=EMBED_COOKIE_NAME,
        value=token,
        max_age=EMBED_TOKEN_TTL_SECONDS,
        # Readable by the browser's request machinery, useless to scripts:
        # HttpOnly keeps a stored XSS payload from lifting it out of
        # document.cookie.
        httponly=True,
        # Lax still sends it on the same-site navigations the dashboard performs
        # while Strict would strip it and break in-app links.
        samesite="lax",
        path="/",
    )
    return {"token": token, "expires_in": EMBED_TOKEN_TTL_SECONDS}


# ---------------------------------------------------------------------------
# Mounted tool proxies (app root)
# ---------------------------------------------------------------------------


def build_proxy_router(tool: str) -> APIRouter:
    """Build the reverse proxy for one tool, to be mounted at ``/<tool>``."""
    upstream_base, sub_path = TOOLS[tool]
    proxy = APIRouter()

    @proxy.get("/__health", include_in_schema=False)
    async def proxy_health() -> dict:
        """Liveness probe so the UI can tell "not running" from "blocked"."""
        return {"tool": tool, "upstream": upstream_base, "sub_path": sub_path}

    # `include_in_schema=False` because a catch-all `/{path}` for a third-party
    # tool's own API says nothing about this service's contract, and because
    # FastAPI derives one operation id per method from the single handler name -
    # six methods, six identically-named operations, one UserWarning per mount.
    @proxy.api_route(
        "/{path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        include_in_schema=False,
    )
    async def proxy_tool(
        path: str,
        request: Request,
        token: str | None = Query(None, description="Embed token from /monitoring/embed-token"),
        credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
    ) -> Response:
        """Stream a request through to the tool with framing allowed.

        Authentication is checked here rather than via ``Depends(require_admin)``
        on purpose. A dependency runs before the body, so requiring a bearer
        token there would reject every iframe request with 401 - the iframe
        cannot send an Authorization header, which is the entire reason the embed
        token exists.

        Two accepted credentials: a normal admin bearer token for direct API
        calls, and the embed token (query or cookie) for the iframe. Keeping them
        separate matters - sharing the bearer token would put it in the tool's
        access logs via the Referer header on every asset request, and would make
        a dashboard URL as sensitive as a session.
        """
        if credentials is not None:
            user = await get_current_user(credentials)
            if user["role"] != "admin":
                raise HTTPException(status_code=403, detail="Admin permission required")
        elif not (
            verify_embed_token(token)
            or verify_embed_token(request.cookies.get(EMBED_COOKIE_NAME))
        ):
            raise HTTPException(status_code=401, detail="Embed token required")

        # The sub-path is part of the upstream request, not just the public mount.
        # Grafana running with serve_from_sub_path redirects anything outside its
        # root_url prefix back onto the prefixed URL - proxying ``/grafana/login``
        # to ``http://grafana:3000/login`` therefore answers every request with a
        # 301 to itself and the browser gives up with ERR_TOO_MANY_REDIRECTS.
        url = f"{upstream_base}{sub_path}/{path}"
        if request.url.query:
            url = f"{url}?{request.url.query}"

        headers = {
            k: v
            for k, v in request.headers.items()
            if k.lower() not in _HOP_BY_HOP and k.lower() != "host"
        }
        # Upstream must build URLs against the proxy, not its own hostname, or
        # redirects and generated links escape the proxy.
        headers["X-Forwarded-Host"] = request.headers.get("host", "")
        headers["X-Forwarded-Proto"] = request.url.scheme
        # Never let the upstream compress. httpx decodes transparently for us but
        # passes the client's Accept-Encoding through, so upstream returns gzip,
        # httpx hands back *plain* bytes still labelled with the original
        # Content-Encoding, and the browser fails with "incorrect header check".
        headers["accept-encoding"] = "identity"

        body = await request.body()

        try:
            async with httpx.AsyncClient(
                timeout=UPSTREAM_TIMEOUT_SECONDS, follow_redirects=False
            ) as client:
                upstream = await client.request(
                    request.method, url, headers=headers, content=body or None
                )
        except httpx.HTTPError as exc:
            logger.warning("Monitoring proxy failed for %s: %s", url, exc)
            raise HTTPException(status_code=502, detail=f"{tool} is unreachable") from exc

        out_headers: dict[str, str] = {}
        for key, value in upstream.headers.multi_items():
            lowered = key.lower()
            if lowered in _HOP_BY_HOP or lowered in _STRIPPED_RESPONSE_HEADERS:
                continue
            # Content-Length is recomputed by Response below; a stale value from
            # upstream would truncate or hang the browser.
            if lowered == "content-length":
                continue
            # Rewrite absolute redirects so the browser stays inside the proxy; a
            # Location: http://grafana:3000/... is unreachable from a browser, and
            # Grafana builds them from the in-network hostname.
            if lowered == "location":
                value = value.replace(f"{upstream_base}{sub_path}", f"/{tool}")
                value = value.replace(upstream_base, f"/{tool}")
            out_headers[key] = value

        return Response(
            content=upstream.content,
            status_code=upstream.status_code,
            headers=out_headers,
        )

    return proxy