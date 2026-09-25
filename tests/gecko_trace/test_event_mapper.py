import datetime
from types import SimpleNamespace

from bigquery_etl.gecko_trace import event_mapper as em
from bigquery_etl.gecko_trace.searchfox import BlameUnavailable

DATE = datetime.date(2026, 9, 25)
GIT_A = "a" * 40
GIT_B = "b" * 40


class FakeJob:
    def __init__(self, rows, affected=None):
        self._rows = rows
        self.num_dml_affected_rows = affected

    def result(self):
        return iter(self._rows)


class FakeBigQuery:
    """Answers queries by a substring key and records loads and MERGEs."""

    def __init__(self, answers):
        self.answers = answers
        self.loads = []
        self.merges = []

    def query(self, sql, job_config=None):
        params = {
            p.name: getattr(p, "values", None) or p.value
            for p in job_config.query_parameters
        }
        if sql.lstrip().startswith("MERGE"):
            self.merges.append(sql)
            return FakeJob([], affected=len(self.merges))
        for key, rows in self.answers.items():
            if key in sql:
                return FakeJob(rows(params) if callable(rows) else rows)
        raise AssertionError(f"unexpected query: {sql[:80]}")

    def load_table_from_json(self, rows, destination, job_config=None):
        self.loads.append((destination, list(rows)))
        return FakeJob([])


class FakeSearchfox:
    def __init__(self, git_by_hg=None, blame=None, unavailable=()):
        self.git_by_hg = git_by_hg or {}
        self.blame = blame or {}
        self.unavailable = set(unavailable)
        self.calls = []

    def resolve_hg_rev(self, tree, hg_rev):
        self.calls.append(("hgrev", tree, hg_rev))
        return self.git_by_hg.get(hg_rev)

    def blame_lines(self, tree, git_rev, selectors):
        wanted = {path: sorted(lines) for path, lines in selectors.items()}
        self.calls.append(("blame", tree, git_rev, wanted))
        if git_rev in self.unavailable:
            raise BlameUnavailable(git_rev)
        return {path: self.blame.get((git_rev, path)) for path in selectors}


def pending_row(
    app_build,
    source_file,
    source_line=10,
    revision_known=False,
    tree=None,
    git_rev=None,
):
    return SimpleNamespace(
        app_build=app_build,
        source_file=source_file,
        source_line=source_line,
        revision_known=revision_known,
        tree=tree,
        git_rev=git_rev,
    )


def buildhub_row(app_build, hg_rev, repository, channel="release"):
    return SimpleNamespace(
        app_build=app_build, channel=channel, hg_rev=hg_rev, repository=repository
    )


def loads_for(bq, table):
    return [rows for destination, rows in bq.loads if destination.endswith(table)]


def test_update_cache_resolves_builds_and_fetches_blame():
    pending = [
        pending_row("20260901", "dom/A.cpp", 10),
        pending_row("20260901", "dom/A.cpp", 20),
        pending_row("20260901", "/builds/worker/checkouts/gecko/dom/B.cpp", 5),
        pending_row("20260902", "dom/A.cpp", 10),  # not in buildhub
        pending_row(
            "20260801",
            "dom/A.cpp",
            10,
            revision_known=True,
            tree="firefox-release",
            git_rev=GIT_A,
        ),
    ]
    bq = FakeBigQuery(
        {
            "gecko_trace_blame_lines_v1": pending,
            "buildhub2": lambda params: [
                buildhub_row(
                    "20260901", "hg1", "https://hg.mozilla.org/releases/mozilla-release"
                )
            ],
        }
    )
    searchfox = FakeSearchfox(
        git_by_hg={"hg1": GIT_B},
        blame={
            (GIT_B, "dom/A.cpp"): [
                {"line": 10, "rev": "o1", "path": "dom/A.cpp", "lineno": 9},
                {"line": 20, "rev": None, "path": None, "lineno": None},
            ],
            (GIT_B, "dom/B.cpp"): None,
            (GIT_A, "dom/A.cpp"): [
                {"line": 10, "rev": "o1", "path": "dom/A.cpp", "lineno": 9}
            ],
        },
    )
    mapper = em.EventMapper(searchfox, project="p", bq_client=bq)
    mapper.update_cache("firefox_desktop", DATE)

    # Only unknown builds are looked up, and every one of them is cached.
    (revisions,) = loads_for(bq, "gecko_trace_source_revisions_v1")
    by_build = {r["app_build"]: r for r in revisions}
    assert set(by_build) == {"20260901", "20260902"}
    assert by_build["20260901"]["tree"] == "firefox-release"
    assert by_build["20260901"]["git_rev"] == GIT_B
    assert by_build["20260901"]["hg_rev"] == "hg1"
    assert by_build["20260902"]["git_rev"] is None
    assert by_build["20260902"]["resolved_date"] == "2026-09-25"
    assert ("hgrev", "firefox-release", "hg1") in searchfox.calls

    # One bulk request per revision, normalized paths, only the event lines.
    blame_calls = [c for c in searchfox.calls if c[0] == "blame"]
    assert ("blame", "firefox-release", GIT_A, {"dom/A.cpp": [10]}) in blame_calls
    assert (
        "blame",
        "firefox-release",
        GIT_B,
        {"dom/A.cpp": [10, 20], "dom/B.cpp": [5]},
    ) in blame_calls

    (blame_rows,) = loads_for(bq, "gecko_trace_blame_lines_v1")
    rows_b = [r for r in blame_rows if r["git_rev"] == GIT_B]
    a_rows = [r for r in rows_b if r["source_file"] == "dom/A.cpp"]
    assert [
        (r["lineno"], r["origin_rev"], r["origin_path"], r["origin_lineno"])
        for r in a_rows
    ] == [
        (10, "o1", "dom/A.cpp", 9),
        (20, None, None, None),  # past the end of the file
    ]
    # The missing file is cached per requested line under its original path.
    b_rows = [r for r in rows_b if r["source_file"].endswith("dom/B.cpp")]
    assert len(b_rows) == 1
    assert b_rows[0]["source_file"] == "/builds/worker/checkouts/gecko/dom/B.cpp"
    assert b_rows[0]["lineno"] == 5 and b_rows[0]["origin_rev"] is None


def test_update_cache_skips_unavailable_blame_and_unresolved_builds():
    pending = [
        pending_row(
            "20260901",
            "dom/A.cpp",
            revision_known=True,
            tree="firefox-main",
            git_rev=GIT_A,
        ),
        pending_row(
            "20260902", "dom/A.cpp", revision_known=True, tree=None, git_rev=None
        ),
    ]
    bq = FakeBigQuery({"gecko_trace_blame_lines_v1": pending})
    searchfox = FakeSearchfox(unavailable={GIT_A})
    mapper = em.EventMapper(searchfox, project="p", bq_client=bq)
    mapper.update_cache("firefox_desktop", DATE)

    assert not [c for c in searchfox.calls if c[0] == "hgrev"]
    assert [c for c in searchfox.calls if c[0] == "blame"] == [
        ("blame", "firefox-main", GIT_A, {"dom/A.cpp": [10]})
    ]
    # Nothing is cached, so the next run tries again.
    assert bq.loads == []


def test_remap_runs_both_merges_and_reports_counts():
    bq = FakeBigQuery({})
    mapper = em.EventMapper(FakeSearchfox(), project="p", bq_client=bq)
    events, traces = mapper.remap("firefox_desktop")
    assert (events, traces) == (1, 2)
    assert "gecko_trace_events_v1` AS T" in bq.merges[0]
    assert "gecko_trace_traces_v1` AS T" in bq.merges[1]
    assert "`p.firefox_desktop_derived." in bq.merges[0]


def test_run_covers_every_application():
    bq = FakeBigQuery({"gecko_trace_blame_lines_v1": []})
    mapper = em.EventMapper(FakeSearchfox(), project="p", bq_client=bq)
    mapper.run(
        reference_date=DATE,
        applications=("firefox_desktop", "org_mozilla_firefox_beta"),
    )
    assert len(bq.merges) == 4
    assert any("org_mozilla_firefox_beta_derived" in m for m in bq.merges)
