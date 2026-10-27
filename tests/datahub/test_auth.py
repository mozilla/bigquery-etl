import json
import os
import socket
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


def fake_browser(error=None, state=None, stray=None):
    """Stand in for the user's browser: follow the authorize URL's redirect.

    stray is a path and query some other local process requests first.
    """

    def open_(authorize_url):
        query = parse_qs(urlparse(authorize_url).query)
        redirect_uri = query["redirect_uri"][0]
        params = {"state": state or query["state"][0]}
        params.update({"error": error} if error else {"code": "the-code"})
        # The browser hits the local server while bqetl waits for it.
        if stray:
            origin = redirect_uri.rsplit("/", 1)[0]
            threading.Timer(0.1, requests.get, args=(origin + stray,)).start()
        callback = f"{redirect_uri}?{urlencode(params)}"
        threading.Timer(0.2, requests.get, args=(callback,)).start()
        return True

    return open_


def test_pkce_verifier_length():
    verifier, _ = auth.pkce_pair()
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

    @patch("bigquery_etl.datahub.auth.LOGIN_TIMEOUT_SECONDS", 1)
    @patch("bigquery_etl.datahub.auth.requests.post")
    @patch("bigquery_etl.datahub.auth.discover", return_value=METADATA)
    def test_ignores_a_mismatched_state(self, discover, post):
        registration = MagicMock(status_code=201)
        registration.json.return_value = {"client_id": "client-1"}
        post.return_value = registration
        with patch(
            "bigquery_etl.datahub.auth.webbrowser.open",
            fake_browser(state="forged"),
        ):
            with pytest.raises(auth.LoginError, match="Timed out"):
                auth.get_token(URL)

    @pytest.mark.parametrize("stray", ["/callback?error=x", "/other?code=c"])
    @patch("bigquery_etl.datahub.auth.requests.post")
    @patch("bigquery_etl.datahub.auth.discover", return_value=METADATA)
    def test_stray_requests_do_not_end_the_wait(self, discover, post, stray):
        registration = MagicMock(status_code=201)
        registration.json.return_value = {"client_id": "client-1"}
        post.side_effect = [registration, token_response()]
        with patch(
            "bigquery_etl.datahub.auth.webbrowser.open", fake_browser(stray=stray)
        ):
            assert auth.get_token(URL) == "access-1"

    @patch("bigquery_etl.datahub.auth.requests.post")
    @patch("bigquery_etl.datahub.auth.discover", return_value=METADATA)
    def test_failed_sign_in_keeps_a_new_registration(self, discover, post):
        registration = MagicMock(status_code=201)
        registration.json.return_value = {"client_id": "client-1"}
        post.return_value = registration
        with patch(
            "bigquery_etl.datahub.auth.webbrowser.open",
            fake_browser(error="access_denied"),
        ):
            with pytest.raises(auth.LoginError):
                auth.get_token(URL)
        cached = json.loads(auth.cache_path().read_text())[URL]
        assert cached["client_id"] == "client-1"

    @patch("bigquery_etl.datahub.auth.requests.post")
    @patch("bigquery_etl.datahub.auth.discover", return_value=METADATA)
    def test_failed_sign_in_forgets_a_reused_registration(self, discover, post):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        redirect_uri = f"http://127.0.0.1:{port}/callback"
        auth._save_cache(
            {URL: {"client_id": "forgotten", "redirect_uri": redirect_uri}}
        )
        with patch(
            "bigquery_etl.datahub.auth.webbrowser.open",
            fake_browser(error="invalid_client"),
        ):
            with pytest.raises(auth.LoginError):
                auth.get_token(URL)
        post.assert_not_called()
        assert json.loads(auth.cache_path().read_text())[URL] == {}

    @patch("bigquery_etl.datahub.auth.requests.post")
    @patch("bigquery_etl.datahub.auth.discover", return_value=METADATA)
    def test_token_response_without_access_token(self, discover, post):
        registration = MagicMock(status_code=201)
        registration.json.return_value = {"client_id": "client-1"}
        no_token = MagicMock(status_code=200)
        no_token.json.return_value = {"error": "server_error"}
        post.side_effect = [registration, no_token]
        with patch("bigquery_etl.datahub.auth.webbrowser.open", fake_browser()):
            with pytest.raises(auth.LoginError, match="access_token"):
                auth.get_token(URL)


@patch("bigquery_etl.datahub.auth.requests.get")
def test_discover_requires_the_endpoints(get):
    get.return_value.json.return_value = {
        k: v for k, v in METADATA.items() if k != "registration_endpoint"
    }
    with pytest.raises(auth.LoginError, match="registration_endpoint"):
        auth.discover(URL)


def test_save_cache_replaces_the_file_owner_only():
    path = auth.cache_path()
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    path.chmod(0o644)
    auth._save_cache({URL: {"access_token": "a"}})
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    assert json.loads(path.read_text()) == {URL: {"access_token": "a"}}
    assert os.listdir(path.parent) == [path.name]

    @patch("bigquery_etl.datahub.auth.requests.post", return_value=token_response())
    @patch("bigquery_etl.datahub.auth.discover", return_value=METADATA)
    def test_reuses_the_registered_port_when_free(self, discover, post):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        redirect_uri = f"http://127.0.0.1:{port}/callback"
        auth._save_cache({URL: {"client_id": "client-1", "redirect_uri": redirect_uri}})

        with patch("bigquery_etl.datahub.auth.webbrowser.open", fake_browser()):
            assert auth.get_token(URL) == "access-1"

        # Only the token request; no new registration.
        (token_call,) = post.call_args_list
        assert token_call.kwargs["data"]["client_id"] == "client-1"
        assert token_call.kwargs["data"]["redirect_uri"] == redirect_uri

    @patch("bigquery_etl.datahub.auth.requests.post")
    @patch("bigquery_etl.datahub.auth.discover", return_value=METADATA)
    def test_registers_again_when_the_port_is_busy(self, discover, post):
        registration = MagicMock(status_code=201)
        registration.json.return_value = {"client_id": "client-2"}
        post.side_effect = [registration, token_response()]

        with socket.socket() as busy:
            busy.bind(("127.0.0.1", 0))
            busy.listen()
            old_uri = f"http://127.0.0.1:{busy.getsockname()[1]}/callback"
            auth._save_cache({URL: {"client_id": "client-1", "redirect_uri": old_uri}})
            with patch("bigquery_etl.datahub.auth.webbrowser.open", fake_browser()):
                assert auth.get_token(URL) == "access-1"

        cached = json.loads(auth.cache_path().read_text())[URL]
        assert cached["client_id"] == "client-2"
        assert cached["redirect_uri"] != old_uri


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
