import subprocess
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from bigquery_etl.deprecation import (
    ALREADY_DEPRECATED,
    GIT_UNAVAILABLE,
    INVALID_METADATA,
    MAX_READ_ONLY_ROWS,
    NO_METADATA,
    UNTRACKED,
    Candidate,
    downstream_candidates,
    is_unused,
    label_text,
    pick,
    picker_sections,
    preselect,
    selection_summary,
    used_reason,
)

PROJECT = "moz-fx-data-shared-prod"


def bq_asset(
    name, degree=1, queries=0, last_query=None, project=PROJECT, created="2020-01-01"
):
    qualified = f"{project}.{name}"
    return {
        "degree": degree,
        "type": "DATASET",
        "subtype": "Table",
        "platform": "bigquery",
        "name": name.split(".")[-1],
        "qualified_name": qualified,
        "urn": f"urn:li:dataset:(urn:li:dataPlatform:bigquery,{qualified},PROD)",
        "deleted": False,
        "queries_30d": queries,
        "users_30d": None,
        "last_query": last_query,
        "created": created,
    }


LOOKER = {
    "degree": 2,
    "type": "DASHBOARD",
    "subtype": None,
    "platform": "looker",
    "name": "Retention",
    "qualified_name": "Shared / Retention",
    "urn": "urn:li:dashboard:(looker,1)",
    "deleted": False,
    "queries_30d": None,
    "users_30d": None,
    "last_query": None,
    "created": None,
}


@pytest.fixture
def sql_dir(tmp_path):
    def table(name, metadata=None):
        path = tmp_path / PROJECT / "d" / name
        path.mkdir(parents=True)
        if metadata is not None:
            (path / "metadata.yaml").write_text(yaml.dump(metadata))

    base = {"friendly_name": "x", "owners": ["test@example.com"]}
    table("target_v1", base)
    table("live_v1", base)
    table("used_v1", base)
    table("old_v1", {**base, "deprecated": True, "deletion_date": "2026-01-01"})
    table("invalid_v1", {**base, "owners": "not a list"})
    table("no_metadata")
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    table("untracked_v1", base)
    return tmp_path


def candidates_for(sql_dir, assets):
    return downstream_candidates(
        assets, str(sql_dir), exclude={sql_dir / PROJECT / "d" / "target_v1"}
    )


class TestDownstreamCandidates:
    def test_splits_bigquery_etl_directories_from_other_assets(self, sql_dir):
        not_in_repo = bq_asset("d.generated_v1")
        candidates, external = candidates_for(
            sql_dir, [bq_asset("d.live_v1"), not_in_repo, LOOKER]
        )
        assert [c.name for c in candidates] == [f"{PROJECT}.d.live_v1"]
        assert external == [not_in_repo, LOOKER]

    def test_drops_the_deprecated_tables(self, sql_dir):
        candidates, _ = candidates_for(sql_dir, [bq_asset("d.target_v1")])
        assert candidates == []

    def test_user_facing_view_joins_its_source_directory(self, sql_dir):
        candidates, _ = candidates_for(
            sql_dir,
            [
                bq_asset("d.live_v1", degree=2, queries=3, last_query="2026-11-01"),
                bq_asset("d.live_v1", queries=4, project="mozdata"),
            ],
        )
        (candidate,) = candidates
        assert candidate.degree == 1
        assert candidate.queries_30d == 7
        assert candidate.last_query == "2026-11-01"

    def test_disabled_reasons(self, sql_dir):
        candidates, _ = candidates_for(
            sql_dir,
            [
                bq_asset("d.live_v1"),
                bq_asset("d.old_v1"),
                bq_asset("d.invalid_v1"),
                bq_asset("d.untracked_v1"),
                bq_asset("d.no_metadata"),
            ],
        )
        assert {c.name: c.disabled_reason for c in candidates} == {
            f"{PROJECT}.d.live_v1": None,
            f"{PROJECT}.d.old_v1": ALREADY_DEPRECATED,
            f"{PROJECT}.d.invalid_v1": INVALID_METADATA,
            f"{PROJECT}.d.untracked_v1": UNTRACKED,
            f"{PROJECT}.d.no_metadata": NO_METADATA,
        }

    def test_nothing_is_selectable_when_git_fails(self, sql_dir, capsys):
        with patch("bigquery_etl.deprecation.tracked_files", return_value=None):
            candidates, _ = candidates_for(sql_dir, [bq_asset("d.live_v1")])
        assert [c.disabled_reason for c in candidates] == [GIT_UNAVAILABLE]
        assert "could not list git-tracked files" in capsys.readouterr().err


class TestSelection:
    def test_unknown_usage_counts_as_used(self):
        assert is_unused(Candidate(Path("a"), [bq_asset("d.a", queries=0)]))
        assert not is_unused(Candidate(Path("a"), [bq_asset("d.a", queries=None)]))
        assert not is_unused(
            Candidate(
                Path("a"), [bq_asset("d.a", queries=0), bq_asset("d.a", queries=None)]
            )
        )

    @pytest.mark.parametrize(
        "mode,expected",
        [("all", {"unused", "used"}), ("unused", {"unused"}), ("none", set())],
    )
    def test_preselect(self, mode, expected):
        candidates = [
            Candidate(Path("unused"), [bq_asset("d.unused", queries=0)]),
            Candidate(Path("used"), [bq_asset("d.used", queries=5)]),
            Candidate(Path("old"), [bq_asset("d.old", queries=0)], ALREADY_DEPRECATED),
        ]
        assert preselect(candidates, mode) == {Path(p) for p in expected}

    @pytest.mark.parametrize(
        "created,reason",
        [
            (date.today().isoformat(), "created in the last 30 days"),
            (None, "creation date unknown"),
            ((date.today() - timedelta(days=31)).isoformat(), None),
        ],
    )
    def test_new_tables_are_not_unused(self, created, reason):
        candidate = Candidate(Path("a"), [bq_asset("d.a", created=created)])
        assert used_reason(candidate) == reason
        assert is_unused(candidate) == (reason is None)

    def test_selection_summary_gives_reasons(self):
        candidates = [
            Candidate(Path("p/d/unused"), [bq_asset("d.unused", queries=0)]),
            Candidate(Path("p/d/used"), [bq_asset("d.used", queries=5)]),
            Candidate(Path("p/d/old"), [bq_asset("d.old")], ALREADY_DEPRECATED),
        ]
        summary = selection_summary(candidates, {Path("p/d/unused")}, "unused")
        assert summary.splitlines() == [
            "Deprecating 1 downstream:",
            "  p.d.unused",
            "Not deprecating 2 downstream:",
            "  p.d.used (5 queries in 30 days)",
            "  p.d.old (already deprecated)",
        ]


class TestPicker:
    @pytest.fixture
    def picker_input(self, sql_dir):
        return candidates_for(
            sql_dir,
            [
                bq_asset("d.used_v1", degree=2, queries=9),
                bq_asset("d.live_v1", queries=0),
                bq_asset("d.old_v1"),
                bq_asset("d.no_metadata"),
                LOOKER,
            ],
        )

    def test_sections_put_selectable_rows_first(self, picker_input):
        sections = picker_sections(*picker_input)
        assert [s.title for s in sections] == [
            "Degree 1 (1)",
            "Degree 2 (1)",
            f"Can't deprecate: {ALREADY_DEPRECATED} (1)",
            f"Can't deprecate: {NO_METADATA} (1)",
            "Not in bigquery-etl: looker (1)",
        ]
        assert [path is not None for s in sections for _, path in s.rows] == [
            True,
            True,
            False,
            False,
            False,
        ]

    def test_rows_line_up_under_their_column_header(self, picker_input):
        for section in picker_sections(*picker_input):
            columns, rows = section.columns, section.rows
            # The last column is left-aligned, so it starts where its header does.
            last = columns.rindex("  ") + 2
            if columns.endswith("LAST QUERY"):
                last = columns.index("LAST QUERY")
            for label, _ in rows:
                text = label_text(label)
                assert text[last - 1] == " " and text[last] != " "

    @pytest.fixture
    def checkbox(self):
        with patch("questionary.checkbox") as checkbox:
            yield checkbox

    def test_checkbox_choices(self, picker_input, checkbox):
        candidates, external = picker_input
        live = candidates[0].path
        checkbox.return_value.ask.return_value = [live]

        result = pick("t", candidates, external, {live}, " (pre-selected: unused)")

        assert result == [live]
        message = checkbox.call_args.args[0]
        assert message == (
            "Downstream of t (pre-selected: unused). Select other assets to deprecate:"
        )
        assert "<ctrl-c> to cancel" in checkbox.call_args.kwargs["instruction"]
        choices = checkbox.call_args.kwargs["choices"]
        rows = [c for c in choices if c.__class__.__name__ == "Choice"]
        assert [(c.checked, bool(c.disabled)) for c in rows] == [
            (True, False),
            (False, False),
            (False, True),
            (False, True),
            (False, True),
        ]
        # Selectable rows are colored by degree, read-only rows are plain.
        assert [c.title[0][0] for c in rows[:2]] == ["fg:ansicyan", "fg:ansiyellow"]
        assert ("fg:ansigreen", "looker  ") in rows[-1].title
        assert "Shared / Retention" in label_text(rows[-1].title)
        headers = [c.title for c in choices if c.__class__.__name__ == "Separator"]
        assert headers[0] == "── Degree 1 (1) ──"
        assert headers[1].split() == ["NAME", "TYPE", "QUERIES", "30D", "LAST", "QUERY"]

    @pytest.mark.parametrize("answer", [[], None])
    def test_empty_selection_and_ctrl_c(self, picker_input, checkbox, answer):
        checkbox.return_value.ask.return_value = answer
        assert pick("t", *picker_input, set()) == answer

    @pytest.mark.parametrize("confirmed,expected", [(True, []), (False, None)])
    def test_nothing_selectable_asks_to_confirm(
        self, sql_dir, checkbox, capsys, confirmed, expected
    ):
        candidates, external = candidates_for(sql_dir, [LOOKER])
        with patch("click.confirm", return_value=confirmed) as confirm:
            assert pick("t", candidates, external, set()) == expected
        checkbox.assert_not_called()
        assert confirm.call_args.args[0] == "Deprecate t?"
        assert "Shared / Retention" in capsys.readouterr().out


def test_read_only_groups_are_capped(sql_dir):
    charts = [
        {**LOOKER, "urn": f"urn:li:dashboard:(looker,{i})", "qualified_name": f"d{i}"}
        for i in range(MAX_READ_ONLY_ROWS + 3)
    ]
    (section,) = picker_sections(*candidates_for(sql_dir, charts))
    rows = section.rows
    assert section.title == f"Not in bigquery-etl: looker ({MAX_READ_ONLY_ROWS + 3})"
    assert len(rows) == MAX_READ_ONLY_ROWS + 1
    assert rows[-1][0].startswith("... and 3 more")
