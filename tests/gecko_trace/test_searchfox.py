from types import SimpleNamespace

import pytest

from bigquery_etl.gecko_trace import searchfox as sf

GIT = "8c206d60bf8ff33097065f5da02172f56864fac8"


class FakeSession:
    """Answers GETs from a dict of url -> (status, headers, body)."""

    def __init__(self, answers):
        self.answers = answers
        self.headers = {}
        self.urls = []

    def get(self, url, timeout=None, allow_redirects=True):
        self.urls.append(url)
        status, headers, body = self.answers[url]
        return SimpleNamespace(
            status_code=status,
            headers=headers,
            text=body if isinstance(body, str) else "",
            json=lambda: body,
        )


def make_client(answers):
    session = FakeSession(answers)
    client = sf.SearchfoxClient(base_url="https://sf.test", session=session, pause=0)
    return client, session


@pytest.mark.parametrize(
    "repository,tree",
    [
        ("https://hg.mozilla.org/mozilla-central", "firefox-main"),
        ("https://hg.mozilla.org/integration/autoland", "firefox-main"),
        ("https://hg.mozilla.org/releases/mozilla-release", "firefox-release"),
        ("https://hg.mozilla.org/releases/mozilla-beta/", "firefox-beta"),
        ("https://hg.mozilla.org/releases/mozilla-esr140", "firefox-esr140"),
        ("https://hg.mozilla.org/projects/cedar", None),
        (None, None),
    ],
)
def test_tree_for_repository(repository, tree):
    assert sf.tree_for_repository(repository) == tree


@pytest.mark.parametrize(
    "source_file,path",
    [
        ("dom/ipc/ContentChild.cpp", "dom/ipc/ContentChild.cpp"),
        (
            "/builds/worker/checkouts/gecko/dom/ipc/ContentChild.cpp",
            "dom/ipc/ContentChild.cpp",
        ),
        ("z:\\build\\build\\src\\netwerk\\Foo.cpp", "netwerk/Foo.cpp"),
        ("./dom/ipc/ContentChild.cpp", "dom/ipc/ContentChild.cpp"),
    ],
)
def test_normalize_source_path(source_file, path):
    assert sf.normalize_source_path(source_file) == path


def test_resolve_hg_rev_follows_redirect():
    client, session = make_client(
        {
            "https://sf.test/firefox-release/hgrev/abc123/": (
                301,
                {"Location": f"/firefox-release/rev/{GIT}/"},
                "",
            ),
            "https://sf.test/firefox-release/hgrev/unknown/": (404, {}, "Not found"),
        }
    )
    assert client.resolve_hg_rev("firefox-release", "abc123") == GIT
    assert client.resolve_hg_rev("firefox-release", "unknown") is None


def test_blame_lines_builds_selectors_and_parses_files():
    body = {
        "rev": GIT,
        "files": [
            {
                "path": "a.cpp",
                "lines": [{"line": 7, "rev": "r1", "path": "a.cpp", "lineno": 1}],
            },
            {"path": "gone.cpp", "lines": None},
            {"path": "whole.cpp", "lines": []},
        ],
    }
    url = f"https://sf.test/t/blame-lines/{GIT}/a.cpp:3:7,gone.cpp:1,whole.cpp"
    client, session = make_client({url: (200, {}, body)})
    result = client.blame_lines(
        "t", GIT, {"gone.cpp": [1], "a.cpp": {7, 3, 7}, "whole.cpp": None}
    )
    assert result == {
        "a.cpp": [{"line": 7, "rev": "r1", "path": "a.cpp", "lineno": 1}],
        "gone.cpp": None,
        "whole.cpp": [],
    }
    assert session.urls == [url]


def test_blame_lines_raises_when_blame_missing():
    client, _ = make_client(
        {
            f"https://sf.test/t/blame-lines/{GIT}/a.cpp:1": (
                500,
                {},
                "Unable to find blame for revision",
            )
        }
    )
    with pytest.raises(sf.BlameUnavailable):
        client.blame_lines("t", GIT, {"a.cpp": [1]})


def test_blame_lines_chunks_long_url_lists(monkeypatch):
    monkeypatch.setattr(sf, "MAX_URL_LENGTH", 120)
    paths = [f"dir/file{i:02d}.cpp" for i in range(6)]
    prefix = f"https://sf.test/t/blame-lines/{GIT}/"

    class ChunkSession(FakeSession):
        def get(self, url, timeout=None, allow_redirects=True):
            self.urls.append(url)
            assert url.startswith(prefix) and len(url) <= 120
            chunk = [sel.split(":")[0] for sel in url[len(prefix) :].split(",")]
            return SimpleNamespace(
                status_code=200,
                headers={},
                text="",
                json=lambda: {
                    "rev": GIT,
                    "files": [{"path": p, "lines": []} for p in chunk],
                },
            )

    session = ChunkSession({})
    client = sf.SearchfoxClient(base_url="https://sf.test", session=session, pause=0)
    result = client.blame_lines("t", GIT, {path: [1, 2] for path in paths})
    assert sorted(result) == paths
    assert len(session.urls) > 1
