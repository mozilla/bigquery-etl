import base64
import hashlib
import json
import os
import stat
import threading
import time
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlencode, urlparse

import pytest
import requests

from bigquery_etl.datahub import auth

URL = "https://datahub.example"
METADATA = {
    "authorization_endpoint": f"{URL}/auth/oauth2/authorize",
    "token_endpoint": f"{URL}/auth/oauth2/token",
    "registration_endpoint": f"{URL}/auth/oauth2/register",
}


@pytest.fixture(autouse=True)
def config_home(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    return tmp_path


def token_response(access_token="access-1", refresh_token="refresh-1", expires_in=3600):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_in": expires_in,
    }
    return resp


def fake_browser(error=None, state=None):
    """Stand in for the user's browser: follow the authorize URL's redirect."""

    def open_(authorize_url):
        query = parse_qs(urlparse(authorize_url).query)
        params = (
            {"error": error}
            if error
            else {"code": "the-code", "state": state or query["state"][0]}
        )
        callback = f"{query['redirect_uri'][0]}?{urlencode(params)}"
        # The browser hits the local server while bqetl waits for it.
        threading.Timer(0.2, requests.get, args=(callback,)).start()
        return True

    return open_


def test_pkce_challenge_is_s256_of_verifier():
    verifier, challenge = auth.pkce_pair()
    digest = hashlib.sha256(verifier.encode()).digest()
    assert challenge == base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    assert 43 <= len(verifier) <= 128


class TestBrowserLogin:
    @patch("bigquery_etl.datahub.auth.requests.post")
    @patch("bigquery_etl.datahub.auth.discover", return_value=METADATA)
    def test_registers_signs_in_and_caches(self, discover, post, config_home):
        registration = MagicMock(status_code=201)
        registration.json.return_value = {"client_id": "client-1"}
        post.side_effect = [registration, token_response()]

        with patch("bigquery_etl.datahub.auth.webbrowser.open", fake_browser()):
            assert auth.get_token(URL) == "access-1"

        register_call, token_call = post.call_args_list
        assert register_call.kwargs["json"]["token_endpoint_auth_method"] == "none"
        redirect_uri = register_call.kwargs["json"]["redirect_uris"][0]
        assert redirect_uri.startswith("http://127.0.0.1:")
        sent = token_call.kwargs["data"]
        assert sent["grant_type"] == "authorization_code"
        assert sent["code"] == "the-code"
        assert sent["client_id"] == "client-1"
        assert sent["redirect_uri"] == redirect_uri
        assert sent["code_verifier"]

        path = auth.cache_path()
        assert path == config_home / "bqetl" / "datahub_auth.json"
        assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
        cached = json.loads(path.read_text())[URL]
        assert cached["client_id"] == "client-1"
        assert cached["refresh_token"] == "refresh-1"

    @patch("bigquery_etl.datahub.auth.requests.post")
    @patch("bigquery_etl.datahub.auth.discover", return_value=METADATA)
    def test_reports_a_denied_sign_in(self, discover, post):
        registration = MagicMock(status_code=201)
        registration.json.return_value = {"client_id": "client-1"}
        post.return_value = registration
        with patch(
            "bigquery_etl.datahub.auth.webbrowser.open",
            fake_browser(error="access_denied"),
        ):
            with pytest.raises(auth.LoginError, match="access_denied"):
                auth.get_token(URL)

    @patch("bigquery_etl.datahub.auth.requests.post")
    @patch("bigquery_etl.datahub.auth.discover", return_value=METADATA)
    def test_rejects_a_mismatched_state(self, discover, post):
        registration = MagicMock(status_code=201)
        registration.json.return_value = {"client_id": "client-1"}
        post.return_value = registration
        with patch(
            "bigquery_etl.datahub.auth.webbrowser.open",
            fake_browser(state="forged"),
        ):
            with pytest.raises(auth.LoginError, match="state"):
                auth.get_token(URL)


class TestCachedTokens:
    def write_cache(self, **entry):
        auth._save_cache({URL: {"client_id": "client-1", **entry}})

    @patch("bigquery_etl.datahub.auth.browser_login")
    def test_valid_token_is_reused(self, browser_login):
        self.write_cache(access_token="cached", expires_at=time.time() + 3600)
        assert auth.get_token(URL) == "cached"
        browser_login.assert_not_called()

    @patch("bigquery_etl.datahub.auth.browser_login")
    @patch(
        "bigquery_etl.datahub.auth.requests.post", return_value=token_response("new")
    )
    @patch("bigquery_etl.datahub.auth.discover", return_value=METADATA)
    def test_expired_token_is_refreshed(self, discover, post, browser_login):
        self.write_cache(
            access_token="old", refresh_token="refresh-1", expires_at=time.time() - 1
        )
        assert auth.get_token(URL) == "new"
        assert post.call_args.kwargs["data"]["grant_type"] == "refresh_token"
        browser_login.assert_not_called()

    @patch("bigquery_etl.datahub.auth.browser_login")
    @patch("bigquery_etl.datahub.auth.requests.post")
    @patch("bigquery_etl.datahub.auth.discover", return_value=METADATA)
    def test_failed_refresh_falls_back_to_browser(self, discover, post, browser_login):
        post.return_value = MagicMock(status_code=400)
        browser_login.side_effect = lambda url, entry: entry.update(
            access_token="from-browser", expires_at=None
        )
        self.write_cache(
            access_token="old", refresh_token="revoked", expires_at=time.time() - 1
        )
        assert auth.get_token(URL) == "from-browser"
        browser_login.assert_called_once()

    def test_clear_tokens_keeps_the_client_registration(self):
        self.write_cache(access_token="a", refresh_token="r", expires_at=1)
        auth.clear_tokens(URL)
        assert json.loads(auth.cache_path().read_text())[URL] == {
            "client_id": "client-1"
        }
