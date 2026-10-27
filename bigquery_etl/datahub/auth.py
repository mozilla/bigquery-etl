"""Sign in to DataHub in the browser, the way MCP clients do.

DataHub Cloud is an OAuth 2.1 authorization server: it publishes its endpoints
at /.well-known/oauth-authorization-server, lets clients register themselves
(dynamic client registration) and supports the authorization code flow with
PKCE. The resulting access token works for the GraphQL API, and a refresh token
renews it without opening the browser again.

This is hand-rolled rather than built on the MCP SDK's OAuthClientProvider:
that one is async-only, needs a newer pydantic plus about ten more packages,
and only refreshes correctly if we patch its internal state.

DATAHUB_GMS_TOKEN, when set, takes precedence over all of this.
"""

import base64
import hashlib
import json
import os
import secrets
import sys
import tempfile
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import requests

CLIENT_NAME = "bqetl"
SCOPE = "openid datahub:account"
LOGIN_TIMEOUT_SECONDS = 300
REQUEST_TIMEOUT_SECONDS = 30
# Renew tokens this long before they expire.
EXPIRY_MARGIN_SECONDS = 60


class LoginError(Exception):
    """The browser sign-in did not complete."""


def cache_path() -> Path:
    """Return the file holding registered clients and tokens, per DataHub instance."""
    config_home = os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config"
    return Path(config_home) / "bqetl" / "datahub_auth.json"


def _load_cache() -> dict[str, Any]:
    try:
        return json.loads(cache_path().read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_cache(cache: dict[str, Any]) -> None:
    path = cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    # mkstemp creates the file readable by the owner only, since it holds tokens.
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(cache, f, indent=2)
        os.replace(tmp, path)
    except BaseException:
        os.unlink(tmp)
        raise


def clear_tokens(url: str) -> None:
    """Forget the tokens for a DataHub instance, keeping its client registration."""
    cache = _load_cache()
    entry = cache.get(url, {})
    for key in ("access_token", "refresh_token", "expires_at"):
        entry.pop(key, None)
    if entry:
        cache[url] = entry
        _save_cache(cache)


def discover(url: str) -> dict[str, Any]:
    """Return DataHub's OAuth authorization server metadata."""
    resp = requests.get(
        f"{url}/.well-known/oauth-authorization-server",
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    metadata = resp.json()
    for key in ("authorization_endpoint", "token_endpoint", "registration_endpoint"):
        if not metadata.get(key):
            raise LoginError(f"DataHub OAuth metadata has no {key}.")
    return metadata


def register_client(registration_endpoint: str, redirect_uri: str) -> str:
    """Register bqetl as a public OAuth client and return its client_id."""
    resp = requests.post(
        registration_endpoint,
        json={
            "client_name": CLIENT_NAME,
            "redirect_uris": [redirect_uri],
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",
            "scope": SCOPE,
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    client_id = resp.json().get("client_id")
    if not client_id:
        raise LoginError("DataHub client registration returned no client_id.")
    return client_id


def pkce_pair() -> tuple[str, str]:
    """Return a PKCE code verifier and its S256 challenge."""
    verifier = secrets.token_urlsafe(64)
    digest = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return verifier, challenge


def _token_request(token_endpoint: str, data: dict[str, str]) -> dict[str, Any]:
    resp = requests.post(token_endpoint, data=data, timeout=REQUEST_TIMEOUT_SECONDS)
    if resp.status_code >= 400:
        raise LoginError(f"DataHub token request failed: HTTP {resp.status_code}")
    tokens = resp.json()
    if not tokens.get("access_token"):
        raise LoginError("DataHub token response has no access_token.")
    return tokens


def _store_tokens(entry: dict[str, Any], tokens: dict[str, Any]) -> None:
    entry["access_token"] = tokens["access_token"]
    if tokens.get("refresh_token"):
        entry["refresh_token"] = tokens["refresh_token"]
    expires_in = tokens.get("expires_in")
    entry["expires_at"] = time.time() + expires_in if expires_in else None


class _CallbackServer(HTTPServer):
    """Local server that receives the authorization code from the browser."""

    params: dict[str, str]
    path: str
    state: str


class _CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 (name defined by BaseHTTPRequestHandler)
        server: _CallbackServer = self.server  # type: ignore[assignment]
        url = urlparse(self.path)
        query = parse_qs(url.query)
        # Ignore anything but DataHub's redirect for this sign-in.
        if (
            url.path != server.path
            or query.get("state") != [server.state]
            or ("code" not in query and "error" not in query)
        ):
            self.send_response(404)
            self.end_headers()
            return
        server.params = {k: v[0] for k, v in query.items()}
        ok = "code" in query
        self.send_response(200 if ok else 400)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        message = (
            "Signed in to DataHub. You can close this tab."
            if ok
            else "DataHub sign-in failed. You can close this tab."
        )
        self.wfile.write(f"<html><body><p>{message}</p></body></html>".encode())

    def log_message(self, *args):
        pass


def _start_callback_server(port: int) -> _CallbackServer:
    server = _CallbackServer(("127.0.0.1", port), _CallbackHandler)
    server.params = {}
    server.path = "/callback"
    return server


def browser_login(url: str, entry: dict[str, Any]) -> None:
    """Sign in through the browser and store the tokens in entry."""
    metadata = discover(url)

    # Reuse the registered redirect port if it's free, else register again.
    server = None
    if entry.get("client_id") and entry.get("redirect_uri"):
        try:
            server = _start_callback_server(urlparse(entry["redirect_uri"]).port or 0)
        except OSError:
            server = None
    reused = server is not None
    if server is None:
        server = _start_callback_server(0)
        redirect_uri = f"http://127.0.0.1:{server.server_port}/callback"
        entry["client_id"] = register_client(
            metadata["registration_endpoint"], redirect_uri
        )
        entry["redirect_uri"] = redirect_uri

    try:
        _sign_in(url, entry, metadata, server)
    except LoginError:
        if reused:
            # DataHub may have dropped this client; register again next time.
            entry.pop("client_id", None)
            entry.pop("redirect_uri", None)
        raise


def _sign_in(
    url: str, entry: dict[str, Any], metadata: dict[str, Any], server: _CallbackServer
) -> None:
    verifier, challenge = pkce_pair()
    state = secrets.token_urlsafe(32)
    server.state = state
    authorize_url = (
        metadata["authorization_endpoint"]
        + "?"
        + urlencode(
            {
                "response_type": "code",
                "client_id": entry["client_id"],
                "redirect_uri": entry["redirect_uri"],
                "scope": SCOPE,
                "state": state,
                "code_challenge": challenge,
                "code_challenge_method": "S256",
                "resource": url,
            }
        )
    )

    print(
        f"Opening the browser to sign in to DataHub. If it doesn't open, visit:\n"
        f"{authorize_url}",
        file=sys.stderr,
    )
    webbrowser.open(authorize_url)

    deadline = time.monotonic() + LOGIN_TIMEOUT_SECONDS
    server.timeout = 1
    try:
        while not server.params and time.monotonic() < deadline:
            server.handle_request()
    finally:
        server.server_close()

    params = server.params
    if not params:
        raise LoginError(
            f"Timed out after {LOGIN_TIMEOUT_SECONDS}s waiting for DataHub sign-in."
        )
    if params.get("error"):
        raise LoginError(
            f"DataHub sign-in failed: {params.get('error_description') or params['error']}"
        )
    tokens = _token_request(
        metadata["token_endpoint"],
        {
            "grant_type": "authorization_code",
            "code": params["code"],
            "redirect_uri": entry["redirect_uri"],
            "client_id": entry["client_id"],
            "code_verifier": verifier,
            "resource": url,
        },
    )
    _store_tokens(entry, tokens)


def _refresh(url: str, entry: dict[str, Any]) -> bool:
    """Renew the access token with the refresh token; return whether it worked."""
    if not entry.get("refresh_token") or not entry.get("client_id"):
        return False
    try:
        tokens = _token_request(
            discover(url)["token_endpoint"],
            {
                "grant_type": "refresh_token",
                "refresh_token": entry["refresh_token"],
                "client_id": entry["client_id"],
                "resource": url,
            },
        )
    except (LoginError, requests.RequestException):
        return False
    _store_tokens(entry, tokens)
    return True


def get_token(url: str) -> str:
    """Return a DataHub access token, signing in through the browser if needed."""
    cache = _load_cache()
    entry = cache.setdefault(url, {})

    expires_at = entry.get("expires_at")
    if entry.get("access_token") and (
        expires_at is None or expires_at - EXPIRY_MARGIN_SECONDS > time.time()
    ):
        return entry["access_token"]

    try:
        if not _refresh(url, entry):
            browser_login(url, entry)
    finally:
        _save_cache(cache)
    return entry["access_token"]
