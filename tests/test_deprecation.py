import importlib.util
import os
import subprocess
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from bigquery_etl.deprecation import (
    MAX_READ_ONLY_ROWS,
    Candidate,
    DeprecatedArtifact,
    DisabledReason,
    IncludeDownstream,
    artifact_paths,
    candidate_label,
    downstream_candidates,
    find_references,
    get_deprecated_artifacts,
    is_unused,
    label_text,
    pick,
    picker_sections,
    preselect,
    selection_summary,
    used_reason,
)

PROJECT = "moz-fx-data-shared-prod"


def bq_asset(name, degree=1, queries=0, last_query=None, project=PROJECT):
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
}


BASE = {"friendly_name": "x", "owners": ["test@example.com"]}


def add_table(sql_dir, name, metadata=BASE, view_of=None):
    """Create a table directory, or a view of view_of when given."""
    path = sql_dir / PROJECT / "d" / name
    path.mkdir(parents=True)
    if metadata is not None:
        (path / "metadata.yaml").write_text(yaml.dump(metadata))
    if view_of:
        (path / "view.sql").write_text(
            f"CREATE OR REPLACE VIEW `{PROJECT}.d.{name}` AS SELECT * FROM `{view_of}`"
        )


def commit(sql_dir, when="2020-01-01T00:00:00"):
    """Commit everything in sql_dir, dated long ago unless when is None."""
    env = dict(os.environ)
    if when:
        env.update(GIT_AUTHOR_DATE=when, GIT_COMMITTER_DATE=when)
    identity = ["-c", "user.name=test", "-c", "user.email=test@example.com"]
    subprocess.run(["git", "add", "."], cwd=sql_dir, check=True)
    subprocess.run(
        ["git", *identity, "-c", "commit.gpgsign=false", "commit", "-q", "-m", "x"],
        cwd=sql_dir,
        check=True,
        env=env,
    )


@pytest.fixture
def sql_dir(tmp_path):
    add_table(tmp_path, "target_v1")
    add_table(tmp_path, "live_v1")
    add_table(tmp_path, "used_v1")
    add_table(
        tmp_path, "old_v1", {**BASE, "deprecated": True, "deletion_date": "2026-01-01"}
    )
    add_table(tmp_path, "invalid_v1", {**BASE, "owners": "not a list"})
    add_table(tmp_path, "no_metadata", None)
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    commit(tmp_path)
    add_table(tmp_path, "untracked_v1")
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
            f"{PROJECT}.d.old_v1": DisabledReason.ALREADY_DEPRECATED,
            f"{PROJECT}.d.invalid_v1": DisabledReason.INVALID_METADATA,
            f"{PROJECT}.d.untracked_v1": DisabledReason.UNTRACKED,
            f"{PROJECT}.d.no_metadata": DisabledReason.NO_METADATA,
        }

    def test_nothing_is_selectable_when_git_fails(self, sql_dir, capsys):
        with patch("bigquery_etl.deprecation.tracked_files", return_value=None):
            candidates, _ = candidates_for(sql_dir, [bq_asset("d.live_v1")])
        assert [c.disabled_reason for c in candidates] == [
            DisabledReason.GIT_UNAVAILABLE
        ]
        assert "could not list git-tracked files" in capsys.readouterr().err


class TestSelection:
    @pytest.mark.parametrize(
        "counts,total,unused",
        [
            ([0], 0, True),
            ([None], None, True),
            ([0, None], 0, True),
            ([None, 1], 1, False),
        ],
    )
    def test_missing_usage_counts_as_no_queries(self, counts, total, unused):
        candidate = Candidate(Path("a"), [bq_asset("d.a", queries=q) for q in counts])
        assert candidate.queries_30d == total
        assert is_unused(candidate) == unused
        if total:
            assert used_reason(candidate) == "1 query in 30 days"

    @pytest.mark.parametrize(
        "mode,expected",
        [
            (IncludeDownstream.ALL, {"unused", "used"}),
            (IncludeDownstream.UNUSED, {"unused"}),
            (IncludeDownstream.NONE, set()),
        ],
    )
    def test_preselect(self, mode, expected):
        candidates = [
            Candidate(Path("unused"), [bq_asset("d.unused", queries=0)]),
            Candidate(Path("used"), [bq_asset("d.used", queries=5)]),
            Candidate(
                Path("old"),
                [bq_asset("d.old", queries=0)],
                DisabledReason.ALREADY_DEPRECATED,
            ),
        ]
        assert preselect(candidates, mode) == {Path(p) for p in expected}

    def test_recently_added_tables_are_not_unused(self, sql_dir):
        add_table(sql_dir, "new_v1")
        commit(sql_dir, when=None)
        old, new = candidates_for(
            sql_dir, [bq_asset("d.live_v1"), bq_asset("d.new_v1")]
        )[0]
        assert (old.is_new, new.is_new) == (False, True)
        assert is_unused(old)
        assert used_reason(new) == "added in the last 30 days"

    def test_age_is_unknown_without_git_history(self, sql_dir):
        with patch("bigquery_etl.deprecation.recently_added", return_value=None):
            (candidate,) = candidates_for(sql_dir, [bq_asset("d.live_v1")])[0]
        assert candidate.is_new

    @pytest.mark.parametrize("view_queries,unused", [(5, set()), (0, {"all"})])
    def test_tables_are_used_through_their_views(self, sql_dir, view_queries, unused):
        # live_v1 <- live (fully qualified) <- live_top (via the user-facing project)
        add_table(sql_dir, "live", view_of=f"{PROJECT}.d.live_v1")
        add_table(sql_dir, "live_top", view_of="mozdata.d.live")
        commit(sql_dir)
        candidates, _ = candidates_for(
            sql_dir,
            [
                bq_asset("d.live_v1"),
                bq_asset("d.live", degree=2),
                bq_asset("d.live_top", degree=3, queries=view_queries),
            ],
        )
        table, view, top = candidates
        dependents = [c.dependent_views for c in (table, view, top)]
        assert dependents == [[view], [top], []]
        selected = preselect(candidates, IncludeDownstream.UNUSED)
        if unused:
            assert selected == {c.path for c in candidates}
            assert candidate_label(table, 0).endswith("none")
        else:
            assert selected == set()
            reason = (
                f"via view {PROJECT}.d.live: via view {PROJECT}.d.live_top: "
                "5 queries in 30 days"
            )
            assert used_reason(table) == reason
            assert candidate_label(table, 0).endswith(reason)

    def test_selection_summary_gives_reasons(self):
        candidates = [
            Candidate(Path("p/d/unused"), [bq_asset("d.unused", queries=0)]),
            Candidate(Path("p/d/used"), [bq_asset("d.used", queries=5)]),
            Candidate(
                Path("p/d/old"), [bq_asset("d.old")], DisabledReason.ALREADY_DEPRECATED
            ),
        ]
        summary = selection_summary(
            candidates, {Path("p/d/unused")}, IncludeDownstream.UNUSED
        )
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
            f"Can't deprecate: {DisabledReason.ALREADY_DEPRECATED.value} (1)",
            f"Can't deprecate: {DisabledReason.NO_METADATA.value} (1)",
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
            if "LAST QUERY" in columns:
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
        assert headers[1].split() == [
            "NAME",
            "TYPE",
            "QUERIES",
            "30D",
            "LAST",
            "QUERY",
            "NOTE",
        ]

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


TODAY = date(2026, 6, 1)


def add_artifact(repo, name, metadata, project=PROJECT, dataset="d"):
    """Create sql/<project>/<dataset>/<name> with a metadata.yaml and query."""
    path = repo / "sql" / project / dataset / name
    path.mkdir(parents=True)
    (path / "metadata.yaml").write_text(yaml.dump(metadata))
    (path / "query.sql").write_text("SELECT 1")
    return path


def deprecated(deletion_date, owners=("a@example.com",)):
    return {
        **BASE,
        "owners": list(owners),
        "deprecated": True,
        "deletion_date": deletion_date,
    }


@pytest.fixture
def repo(tmp_path):
    """A git repo of deprecated and live artifacts, all committed but untracked_v1."""
    add_artifact(tmp_path, "past_v1", deprecated(date(2026, 5, 31)))
    add_artifact(tmp_path, "due_today_v1", deprecated(TODAY))
    add_artifact(tmp_path, "no_date_v1", {**BASE, "deprecated": True})
    add_artifact(tmp_path, "live_v1", BASE)
    add_artifact(tmp_path, "past_v1", deprecated(date(2026, 1, 1)), dataset="d_stable")
    add_artifact(
        tmp_path, "past_v1", deprecated(date(2026, 1, 1)), project="moz-fx-glam-prod"
    )
    add_artifact(tmp_path, "invalid_v1", {**BASE, "owners": "not a list"})
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    commit(tmp_path)
    add_artifact(tmp_path, "untracked_v1", deprecated(date(2026, 1, 1)))
    return tmp_path


def artifact(repo, name, dataset="d", deletion_date=None, owners=("a@example.com",)):
    return DeprecatedArtifact(
        path=repo / "sql" / PROJECT / dataset / name,
        deletion_date=deletion_date,
        owners=list(owners),
    )


class TestDeprecatedArtifact:
    @pytest.mark.parametrize(
        "deletion_date, past_due",
        [(date(2026, 5, 31), True), (TODAY, False), (None, False)],
    )
    def test_is_past_due(self, tmp_path, deletion_date, past_due):
        artifact_ = artifact(tmp_path, "t_v1", deletion_date=deletion_date)

        assert artifact_.is_past_due(TODAY) is past_due

    def test_deprecated_artifact_rows(self, repo):
        rows = {
            (r["project_id"], r["dataset_id"], r["table_id"]): r
            for r in (a.to_row() for a in get_deprecated_artifacts(str(repo / "sql")))
        }

        assert rows[(PROJECT, "d", "past_v1")] == {
            "project_id": PROJECT,
            "dataset_id": "d",
            "table_id": "past_v1",
            "deletion_date": "2026-05-31",
            "owners": ["a@example.com"],
        }
        assert rows[(PROJECT, "d", "no_date_v1")]["deletion_date"] is None
        # live_v1 isn't deprecated and invalid_v1 can't be parsed
        assert (PROJECT, "d", "live_v1") not in rows
        assert (PROJECT, "d", "invalid_v1") not in rows


def load_deprecated_tables_job():
    """Import deprecated_tables_v1/query.py, which isn't in a package."""
    path = (
        Path(__file__).parent.parent
        / "sql/moz-fx-data-shared-prod/monitoring_derived/deprecated_tables_v1/query.py"
    )
    spec = importlib.util.spec_from_file_location("deprecated_tables_job", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestDeprecatedTablesJob:
    def test_reports_artifacts_past_their_deletion_date(self, repo, monkeypatch):
        job = load_deprecated_tables_job()
        monkeypatch.setenv("SLACK_API_KEY", "token")
        sql_dir = str(repo / "sql")

        with (
            patch.object(job, "date") as mock_date,
            patch.object(job, "bigquery"),
            patch.object(job, "send_slack_message"),
            patch.object(job, "format_slack_message", return_value="msg") as fmt,
        ):
            mock_date.today.return_value = TODAY
            result = CliRunner().invoke(job.main, [f"--sql-dir={sql_dir}"])

        assert result.exit_code == 0, result.output
        reported = {
            f"{r['project_id']}.{r['dataset_id']}.{r['table_id']}"
            for r in fmt.call_args.args[1]
        }
        # due_today_v1 is on its deletion date, not past it
        assert reported == {
            f"{PROJECT}.d.past_v1",
            f"{PROJECT}.d_stable.past_v1",
            "moz-fx-glam-prod.d.past_v1",
            f"{PROJECT}.d.untracked_v1",
        }


class TestArtifactPaths:
    def test_artifact_paths_include_tests_mirror_only_if_present(self, repo):
        with_tests = artifact(repo, "past_v1")
        mirror = repo / "tests" / "sql" / PROJECT / "d" / "past_v1"
        mirror.mkdir(parents=True)

        assert artifact_paths(with_tests) == [with_tests.path, mirror]
        assert artifact_paths(artifact(repo, "live_v1")) == [
            repo / "sql" / PROJECT / "d" / "live_v1"
        ]


class TestFindReferences:
    def test_finds_tracked_references_outside_deleted_paths(self, repo):
        sql = repo / "sql" / PROJECT / "d"
        (sql / "live_v1" / "query.sql").write_text("SELECT * FROM d.past_v1")
        (sql / "past_v1" / "query.sql").write_text("SELECT * FROM d.past_v1")
        # a longer name sharing the prefix isn't a reference
        (sql / "no_date_v1" / "query.sql").write_text("SELECT * FROM d.past_v10")
        mirror = repo / "tests" / "sql" / PROJECT / "d" / "past_v1"
        mirror.mkdir(parents=True)
        (mirror / "expect.yaml").write_text("d.past_v1")
        # paths count too, e.g. in bqetl_project.yaml skip lists
        (repo / "bqetl_project.yaml").write_text(f"- sql/{PROJECT}/d/past_v1/query.sql")
        commit(repo, when=None)
        (sql / "live_v1" / "untracked.sql").write_text("SELECT * FROM d.past_v1")

        references = find_references([artifact(repo, "past_v1")])

        # paths are relative to the repo root
        assert references == {
            f"{PROJECT}.d.past_v1": [
                Path("bqetl_project.yaml"),
                Path("sql") / PROJECT / "d" / "live_v1" / "query.sql",
            ]
        }

    def test_returns_none_outside_git(self, tmp_path):
        add_artifact(tmp_path, "past_v1", deprecated(date(2026, 1, 1)))

        assert find_references([artifact(tmp_path, "past_v1")]) is None
