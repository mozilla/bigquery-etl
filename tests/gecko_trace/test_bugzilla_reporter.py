import datetime
from types import SimpleNamespace

import pytest

from bigquery_etl.gecko_trace import bugzilla_reporter as br

REFERENCE_DATE = datetime.date(2026, 9, 28)
TRACE = "0123456789abcdef-trace"


class FakeJob:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return iter(self._rows)


class FakeBigQuery:
    """Answers each reporter query from a table-name keyed dict and records DML."""

    def __init__(self, answers):
        self.answers = answers
        self.dml = []

    def query(self, sql, job_config=None):
        params = {p.name: p.value for p in job_config.query_parameters}
        if sql.lstrip().startswith(("INSERT", "UPDATE")):
            self.dml.append((sql.split()[0], params))
            return FakeJob([])
        for key, rows in self.answers.items():
            if key in sql:
                return FakeJob(rows(params) if callable(rows) else rows)
        raise AssertionError(f"unexpected query: {sql[:80]}")


class FakeBugzilla:
    def __init__(self, status=("NEW", "")):
        self.filed = []
        self.comments = []
        self.status = status
        self.next_id = 1000

    def file_bug(self, summary, description):
        self.next_id += 1
        self.filed.append((self.next_id, summary, description))
        return self.next_id

    def get_status(self, bug_id):
        return self.status

    def add_comment(self, bug_id, text):
        self.comments.append((bug_id, text))


def stats_answers(new_traces, known):
    """Common view answers: only firefox_desktop has data."""

    def only_desktop(rows):
        return lambda params: rows if params["app_id"] == "firefox_desktop" else []

    return {
        "gecko_trace_aggregates.bug_reports` br": only_desktop(known),
        "gecko_trace_aggregates.traces`\nWHERE\n  app_id = @app_id\n  AND stable_trace_id NOT IN": only_desktop(
            new_traces
        ),
        "gecko_trace_aggregates.traces_daily": [
            SimpleNamespace(
                hit_count=42,
                avg_duration_nano=1_500_000.0,
                first_date=datetime.date(2026, 9, 22),
                last_date=REFERENCE_DATE,
            )
        ],
        "gecko_trace_aggregates.platform_counts": [
            SimpleNamespace(
                app_build="20260920000000",
                normalized_os="Windows",
                normalized_os_version="10.0",
                architecture="x86_64",
                hit_count=40,
            )
        ],
        "ORDER BY\n  last_seen_date DESC": [SimpleNamespace(trace_signature="sigB")],
        "gecko_trace_aggregates.trace_events": [
            SimpleNamespace(
                event_position=1,
                source_file="dom/ipc/ContentChild.cpp",
                source_line=3160,
                result="NS_OK",
            ),
            SimpleNamespace(
                event_position=2,
                source_file="netwerk/protocol/http/HttpChannelChild.cpp",
                source_line=530,
                result="NS_ERROR_FAILURE",
            ),
        ],
    }


def make_reporter(new_traces=(), known=(), bugzilla=None):
    bq = FakeBigQuery(stats_answers(list(new_traces), list(known)))
    bz = bugzilla or FakeBugzilla()
    return br.BugzillaReporter(bz, project="proj", bq_client=bq), bq, bz


def known_row(**overrides):
    row = dict(
        stable_trace_id=TRACE,
        bug_id=555,
        last_comment_date=datetime.date(2026, 9, 14),
        last_reported_trace_signature="sigB",
    )
    row.update(overrides)
    return SimpleNamespace(**row)


def test_new_trace_files_one_bug_and_records_it():
    reporter, bq, bz = make_reporter(
        new_traces=[SimpleNamespace(stable_trace_id=TRACE)]
    )
    reporter.report(REFERENCE_DATE)

    assert len(bz.filed) == 1
    bug_id, summary, description = bz.filed[0]
    assert summary == f"[gecko-trace] New trace pattern {TRACE[:12]} in firefox_desktop"
    assert "Occurrences: 42" in description
    assert "Average duration: 1.50 ms" in description
    assert "Windows 10.0 / x86_64 (build 20260920000000) -- 40 hits" in description
    assert (
        "1. https://searchfox.org/firefox-main/source/dom/ipc/ContentChild.cpp#3160"
        in description
    )
    assert bz.comments == []
    assert bq.dml == [
        (
            "INSERT",
            {
                "app_id": "firefox_desktop",
                "stable_trace_id": TRACE,
                "bug_id": bug_id,
                "reference_date": REFERENCE_DATE,
                "trace_signature": "sigB",
            },
        )
    ]


def test_known_active_trace_gets_comment_not_new_bug():
    reporter, bq, bz = make_reporter(known=[known_row()])
    reporter.report(REFERENCE_DATE)

    assert bz.filed == []
    assert len(bz.comments) == 1
    bug_id, text = bz.comments[0]
    assert bug_id == 555
    assert text.startswith("Weekly gecko-trace update.")
    assert "source locations changed" not in text
    assert bq.dml == [
        (
            "UPDATE",
            {
                "app_id": "firefox_desktop",
                "stable_trace_id": TRACE,
                "reference_date": REFERENCE_DATE,
                "trace_signature": "sigB",
            },
        )
    ]


def test_signature_change_is_called_out():
    reporter, _, bz = make_reporter(
        known=[known_row(last_reported_trace_signature="sigA")]
    )
    reporter.report(REFERENCE_DATE)

    assert "source locations changed since the last report" in bz.comments[0][1]


def test_recent_comment_is_not_repeated():
    reporter, bq, bz = make_reporter(
        known=[known_row(last_comment_date=REFERENCE_DATE - datetime.timedelta(days=3))]
    )
    reporter.report(REFERENCE_DATE)

    assert bz.comments == []
    assert bq.dml == []


def test_resolved_bug_is_commented_but_not_reopened():
    bz = FakeBugzilla(status=("RESOLVED", "FIXED"))
    reporter, _, _ = make_reporter(known=[known_row()], bugzilla=bz)
    reporter.report(REFERENCE_DATE)

    assert len(bz.comments) == 1
    assert (
        "This bug is RESOLVED FIXED, but the trace pattern is still seen."
        in bz.comments[0][1]
    )
    assert not hasattr(bz, "updated_status")


def test_failures_are_counted_and_raised():
    class FailingBugzilla(FakeBugzilla):
        def file_bug(self, summary, description):
            raise RuntimeError("bugzilla down")

    reporter, bq, _ = make_reporter(
        new_traces=[SimpleNamespace(stable_trace_id=TRACE)], bugzilla=FailingBugzilla()
    )
    with pytest.raises(RuntimeError, match="1 trace"):
        reporter.report(REFERENCE_DATE)
    assert bq.dml == []
