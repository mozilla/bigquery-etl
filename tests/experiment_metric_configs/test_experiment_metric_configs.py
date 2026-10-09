"""Tests for resolving and reusing metric configs in experiment_metric_configs_v1/query.py."""

import datetime
import importlib.util
from pathlib import Path
from textwrap import dedent
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from git import Repo
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery
from metric_config_parser.config import ConfigCollection, LocalConfigCollection

from bigquery_etl.experiments.experimenter import Branch, NimbusExperiment, Outcome

QUERY_DIR = (
    Path(__file__).resolve().parents[2]
    / "sql"
    / "moz-fx-data-experiments"
    / "monitoring"
    / "experiment_metric_configs_v1"
)
QUERY_PY = QUERY_DIR / "query.py"
FIXTURE_DIR = Path(__file__).parent / "metric_hub_fixture"

# In CI the sql/ directory may be replaced with generated SQL,
# which removes Python query files. Skip in that case.
if not QUERY_PY.exists():
    pytest.skip("query.py not available (sql/ replaced in CI)", allow_module_level=True)


def load_query_module():
    # Load the module dynamically since the path contains hyphens.
    spec = importlib.util.spec_from_file_location(
        "experiment_metric_configs_query", QUERY_PY
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def query():
    return load_query_module()


@pytest.fixture(scope="module")
def configs():
    return LocalConfigCollection.from_local_path(str(FIXTURE_DIR))


UTC = datetime.UTC
NOW = datetime.datetime(2026, 10, 8, tzinfo=UTC)
FIRST_UPDATED = datetime.datetime(2026, 1, 1, tzinfo=UTC)
FINGERPRINT = "fingerprint"
ENDED_LONG_AGO = NOW - datetime.timedelta(days=60)
ENDED_OVER_90_DAYS_AGO = NOW - datetime.timedelta(days=100)


def make_nimbus_experiment(slug, outcomes=None, end_date=None, is_rollout=False):
    return NimbusExperiment(
        slug=slug,
        startDate=datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC),
        endDate=end_date,
        enrollmentEndDate=None,
        proposedEnrollment=7,
        branches=[Branch(slug="control", ratio=1, features=None)],
        referenceBranch="control",
        appName="firefox_desktop",
        appId="firefox-desktop",
        channel="release",
        channels=["release"],
        targeting="true",
        bucketConfig={
            "namespace": "ns",
            "randomizationUnit": "normandy_id",
            "start": 0,
            "count": 100,
            "total": 10000,
        },
        featureIds=[],
        isRollout=is_rollout,
        outcomes=outcomes,
    )


class TestResolveMetricConfig:
    def test_partial_metric_failure_keeps_good_metrics(self, query, configs):
        # defaults/firefox_desktop.toml has good_metric and bad_metric (no
        # statistical treatment, so it fails to resolve).
        ne = make_nimbus_experiment("no-external-config-experiment")
        metric_config = query.resolve_metric_config(ne, configs)

        assert metric_config.has_external_config is False
        assert metric_config.resolution_error is None

        metric_names = {metric.name for metric in metric_config.metrics}
        assert "good_metric" in metric_names
        assert "bad_metric" not in metric_names

        unresolved_names = {m.name for m in metric_config.unresolved_metrics}
        assert unresolved_names == {"bad_metric"}

    def test_external_config_overrides(self, query, configs):
        # override-experiment.toml sets [experiment] end_date and enrollment_period.
        ne = make_nimbus_experiment(
            "override-experiment",
            end_date=datetime.datetime(2024, 8, 1, tzinfo=datetime.UTC),
        )
        metric_config = query.resolve_metric_config(ne, configs)

        assert metric_config.has_external_config is True
        assert metric_config.has_external_config_overrides is True
        assert metric_config.overrides.end_date == "2024-06-01"
        assert metric_config.overrides.enrollment_period == 21
        assert metric_config.overrides.reference_branch is None
        assert metric_config.overrides.start_date is None

    def test_unresolved_outcome(self, query, configs):
        ne = make_nimbus_experiment(
            "outcome-missing-experiment",
            outcomes=[Outcome(slug="nonexistent-outcome")],
        )
        metric_config = query.resolve_metric_config(ne, configs)
        assert metric_config.unresolved_outcomes == ["nonexistent-outcome"]

    def test_known_outcome_resolves(self, query, configs):
        # known_outcome contributes no metrics; this only covers the
        # merge_outcome/merge_parameters success path.
        ne = make_nimbus_experiment(
            "outcome-present-experiment",
            outcomes=[Outcome(slug="known_outcome")],
        )
        metric_config = query.resolve_metric_config(ne, configs)
        assert metric_config.unresolved_outcomes == []

    def test_whole_experiment_resolution_failure(self, query, configs):
        # segment-failure-experiment.toml references a missing segment, which
        # fails outside the per-metric try/except.
        ne = make_nimbus_experiment("segment-failure-experiment")
        with pytest.raises(query.RECOVERABLE_RESOLUTION_ERRORS):
            query.resolve_metric_config(ne, configs)


class TestGetMetricConfigs:
    def test_bad_config_does_not_fail_the_run(self, query, configs):
        experiments = [
            make_nimbus_experiment("segment-failure-experiment"),
            make_nimbus_experiment("no-external-config-experiment"),
        ]
        rows = query.get_metric_configs(experiments, configs, {}, {}, NOW, FINGERPRINT)

        by_slug = {row["normandy_slug"]: row for row in rows}
        failed = by_slug["segment-failure-experiment"]["metric_config"]
        assert failed["resolution_error"] is not None
        # segment-failure-experiment.toml is a real external config; it just
        # fails to resolve, so has_external_config must still read True.
        assert failed["has_external_config"] is True
        assert failed["has_external_config_overrides"] is None

        assert (
            by_slug["no-external-config-experiment"]["metric_config"][
                "resolution_error"
            ]
            is None
        )


def previous_row(query, **overrides):
    values = {
        "computed_at": NOW - datetime.timedelta(days=1),
        "config_as_of": FIRST_UPDATED,
        "resolver_fingerprint": FINGERPRINT,
        "needs_retry": False,
        "blob": {"normandy_slug": "slug-a"},
    }
    return query.PreviousRow(**{**values, **overrides})


def reuse_case(id, previous_overrides, end_date, first_updated, expected, force=False):
    return pytest.param(
        previous_overrides, end_date, first_updated, force, expected, id=id
    )


class TestCanReuseRow:
    @pytest.mark.parametrize(
        "previous_overrides, end_date, first_updated, force_refresh, expected",
        [
            reuse_case("reusable", {}, ENDED_LONG_AGO, FIRST_UPDATED, True),
            reuse_case("no previous row", None, ENDED_LONG_AGO, FIRST_UPDATED, False),
            reuse_case(
                "force refresh", {}, ENDED_LONG_AGO, FIRST_UPDATED, False, force=True
            ),
            reuse_case(
                "HEAD-resolved row, still no first_updated",
                {"config_as_of": None},
                ENDED_LONG_AGO,
                None,
                False,
            ),
            reuse_case(
                "previously resolved against HEAD",
                {"config_as_of": None},
                ENDED_LONG_AGO,
                FIRST_UPDATED,
                False,
            ),
            reuse_case(
                "first_updated changed",
                {},
                ENDED_LONG_AGO,
                FIRST_UPDATED + datetime.timedelta(days=1),
                False,
            ),
            reuse_case(
                "fingerprint changed",
                {"resolver_fingerprint": "old"},
                ENDED_LONG_AGO,
                FIRST_UPDATED,
                False,
            ),
            reuse_case(
                "previous error to retry",
                {"needs_retry": True},
                ENDED_LONG_AGO,
                FIRST_UPDATED,
                False,
            ),
            reuse_case(
                "row 7 days old",
                {"computed_at": NOW - datetime.timedelta(days=7)},
                ENDED_LONG_AGO,
                FIRST_UPDATED,
                False,
            ),
            reuse_case("still live", {}, None, FIRST_UPDATED, False),
            reuse_case(
                "ended 29 days ago",
                {},
                NOW - datetime.timedelta(days=29),
                FIRST_UPDATED,
                False,
            ),
            reuse_case(
                "ended 90 days ago, old row",
                {"computed_at": NOW - datetime.timedelta(days=60)},
                NOW - datetime.timedelta(days=90),
                FIRST_UPDATED,
                True,
            ),
            reuse_case(
                "ended 89 days ago, old row",
                {"computed_at": NOW - datetime.timedelta(days=60)},
                NOW - datetime.timedelta(days=89),
                FIRST_UPDATED,
                False,
            ),
            reuse_case(
                "ended over 90 days ago, first_updated changed",
                {"computed_at": NOW - datetime.timedelta(days=60)},
                ENDED_OVER_90_DAYS_AGO,
                FIRST_UPDATED + datetime.timedelta(days=1),
                False,
            ),
        ],
    )
    def test_can_reuse_row(
        self,
        query,
        previous_overrides,
        end_date,
        first_updated,
        force_refresh,
        expected,
    ):
        previous = (
            None
            if previous_overrides is None
            else previous_row(query, **previous_overrides)
        )
        experiment = make_nimbus_experiment("slug-a", end_date=end_date)
        assert (
            query.can_reuse_row(
                previous, experiment, first_updated, NOW, FINGERPRINT, force_refresh
            )
            is expected
        )


class TestReadPreviousRows:
    def test_converts_rows_for_reload_and_flags_errors(self, query):
        row = MagicMock(
            normandy_slug="slug-a",
            computed_at=NOW,
            config_as_of=FIRST_UPDATED,
            resolver_fingerprint=FINGERPRINT,
            metric_config={
                "resolution_error": None,
                "overrides": [{"start_date": datetime.date(2026, 1, 2)}],
            },
        )
        error_row = MagicMock(
            normandy_slug="slug-b",
            computed_at=NOW,
            config_as_of=None,
            resolver_fingerprint=FINGERPRINT,
            metric_config={"resolution_error": "boom"},
        )
        load_failed_row = MagicMock(
            normandy_slug="slug-c",
            computed_at=NOW,
            config_as_of=FIRST_UPDATED,
            resolver_fingerprint=FINGERPRINT,
            metric_config={"resolution_error": f"{query.AS_OF_ERROR_PREFIX}boom"},
        )
        unexpected_error_row = MagicMock(
            normandy_slug="slug-d",
            computed_at=NOW,
            config_as_of=FIRST_UPDATED,
            resolver_fingerprint=FINGERPRINT,
            metric_config={"resolution_error": f"{query.UNEXPECTED_ERROR_PREFIX}boom"},
        )
        client = MagicMock()
        client.query.return_value.result.return_value = [
            row,
            error_row,
            load_failed_row,
            unexpected_error_row,
        ]

        previous_rows = query.read_previous_rows(client, "proj.monitoring.table")
        previous = previous_rows["slug-a"]
        # only failing to load metric-hub is retried, not a config that fails to resolve
        assert previous_rows["slug-b"].needs_retry is False
        assert previous_rows["slug-c"].needs_retry is True
        assert previous_rows["slug-d"].needs_retry is True

        assert previous.computed_at == NOW
        assert previous.config_as_of == FIRST_UPDATED
        assert previous.resolver_fingerprint == FINGERPRINT
        assert previous.needs_retry is False
        assert previous.blob == {
            "normandy_slug": "slug-a",
            "computed_at": NOW.isoformat(),
            "config_as_of": FIRST_UPDATED.isoformat(),
            "resolver_fingerprint": FINGERPRINT,
            "metric_config": {
                "resolution_error": None,
                "overrides": [{"start_date": "2026-01-02"}],
            },
        }

    def test_ignores_rows_without_metric_config(self, query):
        rows = [
            MagicMock(
                normandy_slug="slug-a",
                computed_at=NOW,
                config_as_of=FIRST_UPDATED,
                metric_config=None,
            ),
            MagicMock(
                normandy_slug="slug-b",
                computed_at=NOW,
                config_as_of=FIRST_UPDATED,
                metric_config={"resolution_error": None},
            ),
        ]
        client = MagicMock()
        client.query.return_value.result.return_value = rows

        assert list(query.read_previous_rows(client, "t")) == ["slug-b"]

    @pytest.mark.parametrize(
        "error",
        [NotFound("no table"), BadRequest("no config_as_of column")],
        ids=["missing table", "table without config_as_of"],
    )
    def test_unreadable_table_returns_nothing(self, query, error):
        client = MagicMock()
        client.query.side_effect = error
        assert query.read_previous_rows(client, "t") == {}


class TestGetFirstUpdatedByNormalizedSlug:
    def test_uses_earliest_label_and_skips_tables_without_one(self, query):
        client = MagicMock()
        client.query.return_value.result.return_value = [
            MagicMock(
                table_name="enrollments_slug_a",
                last_updated=["1700000100", "1700000000"],
            ),
            MagicMock(table_name="enrollments_slug_b", last_updated=[]),
        ]

        result = query.get_first_updated_by_normalized_slug(client, "proj")

        assert result == {"slug_a": datetime.datetime.fromtimestamp(1700000000, UTC)}

    def test_warns_when_no_labels_are_found(self, query, caplog):
        client = MagicMock()
        client.query.return_value.result.return_value = [
            MagicMock(table_name="enrollments_slug_a", last_updated=[])
        ]

        with caplog.at_level("WARNING"):
            assert query.get_first_updated_by_normalized_slug(client, "proj") == {}

        assert "No last_updated labels found" in caplog.text


class TestComputeFingerprint:
    def fields(self, description="d", mode="NULLABLE", nested_type="INTEGER", extra=()):
        return [
            bigquery.SchemaField("a", "STRING", mode=mode, description=description),
            bigquery.SchemaField(
                "b", "RECORD", fields=[bigquery.SchemaField("c", nested_type)]
            ),
            *extra,
        ]

    def test_ignores_descriptions(self, query):
        assert query.compute_fingerprint(self.fields("x")) == query.compute_fingerprint(
            self.fields("y")
        )

    @pytest.mark.parametrize(
        "changes",
        [
            {"nested_type": "STRING"},
            {"extra": (bigquery.SchemaField("d", "STRING"),)},
        ],
        ids=["nested field type", "new field"],
    )
    def test_changes_with_schema_structure(self, query, changes):
        assert query.compute_fingerprint(
            self.fields(**changes)
        ) != query.compute_fingerprint(self.fields())

    def test_changes_with_resolver_version(self, query):
        first = query.compute_fingerprint(self.fields())
        with patch.object(query, "RESOLVER_VERSION", query.RESOLVER_VERSION + 1):
            second = query.compute_fingerprint(self.fields())
        assert first != second

    def test_changes_with_library_version(self, query):
        with patch.object(query.importlib.metadata, "version", return_value="1"):
            first = query.compute_fingerprint(self.fields())
        with patch.object(query.importlib.metadata, "version", return_value="2"):
            second = query.compute_fingerprint(self.fields())
        assert first != second


COMMITS = [  # newest first, like iter_commits
    SimpleNamespace(
        hexsha="c3", committed_datetime=datetime.datetime(2026, 3, 1, tzinfo=UTC)
    ),
    SimpleNamespace(
        hexsha="c2", committed_datetime=datetime.datetime(2026, 2, 1, tzinfo=UTC)
    ),
    SimpleNamespace(
        hexsha="c1", committed_datetime=datetime.datetime(2026, 1, 1, tzinfo=UTC)
    ),
]

# slug_a and slug_b resolve to commit c2, slug_c to c1
FIRST_UPDATED_BY_SLUG = {
    "slug_a": datetime.datetime(2026, 2, 5, tzinfo=UTC),
    "slug_b": datetime.datetime(2026, 2, 20, tzinfo=UTC),
    "slug_c": datetime.datetime(2026, 1, 15, tzinfo=UTC),
}


def make_fake_configs(commit_hash_for_as_of=None):
    """A fake ConfigCollection whose as_of picks the newest commit not after ts."""
    repo = SimpleNamespace(commit_hash="HEAD", repo=MagicMock())
    repo.repo.iter_commits.return_value = COMMITS
    configs = SimpleNamespace(repos=[repo], configs=[], as_of=MagicMock())

    def as_of(timestamp):
        picked = next(c for c in COMMITS if c.committed_datetime <= timestamp)
        repo.commit_hash = commit_hash_for_as_of or picked.hexsha
        return SimpleNamespace(repos=[repo], configs=[])

    configs.as_of.side_effect = as_of
    return configs


def ended_experiments(*slugs):
    return [make_nimbus_experiment(s, end_date=ENDED_LONG_AGO) for s in slugs]


def get_rows(query, experiments, configs, first_updated=None, previous=None):
    return query.get_metric_configs(
        experiments,
        configs,
        FIRST_UPDATED_BY_SLUG if first_updated is None else first_updated,
        previous or {},
        NOW,
        FINGERPRINT,
    )


@pytest.fixture
def resolve(query):
    with patch.object(
        query, "resolve_metric_config", return_value=query.MetricConfig()
    ) as resolve:
        yield resolve


@pytest.mark.usefixtures("resolve")
class TestGetMetricConfigsReuse:
    def test_rollouts_are_skipped(self, query):
        experiments = [
            make_nimbus_experiment("slug-a", end_date=ENDED_LONG_AGO),
            make_nimbus_experiment("slug-r", end_date=ENDED_LONG_AGO, is_rollout=True),
        ]

        blobs = get_rows(query, experiments, make_fake_configs())

        assert [b["normandy_slug"] for b in blobs] == ["slug-a"]

    def test_experiments_without_timestamp_use_head_configs(self, query, resolve):
        configs = make_fake_configs()

        blobs = get_rows(query, [make_nimbus_experiment("slug-d")], configs, {})

        configs.as_of.assert_not_called()
        assert resolve.call_args.args[1] is configs
        assert blobs[0]["config_as_of"] is None

    def test_falls_back_to_per_timestamp_when_as_of_picks_older_commit(self, query):
        configs = make_fake_configs(commit_hash_for_as_of="c1")

        get_rows(query, ended_experiments("slug-a", "slug-b"), configs)

        assert configs.as_of.call_count == 2

    def test_config_errors_are_kept_but_unexpected_errors_are_marked_for_retry(
        self, query, resolve
    ):
        resolve.side_effect = [ValueError("bad config"), TypeError("oops")]

        blobs = get_rows(
            query, ended_experiments("slug-a", "slug-b"), make_fake_configs()
        )

        assert [b["metric_config"]["resolution_error"] for b in blobs] == [
            "bad config",
            f"{query.UNEXPECTED_ERROR_PREFIX}oops",
        ]

    def test_as_of_failure_becomes_error_rows_for_the_group(self, query, resolve):
        configs = make_fake_configs()
        real_as_of = configs.as_of.side_effect

        def as_of(timestamp):
            if timestamp.month == 2:
                raise RuntimeError("boom")
            return real_as_of(timestamp)

        configs.as_of.side_effect = as_of

        blobs = get_rows(
            query, ended_experiments("slug-a", "slug-b", "slug-c"), configs
        )

        assert [b["metric_config"]["resolution_error"] for b in blobs[:2]] == [
            f"{query.AS_OF_ERROR_PREFIX}boom"
        ] * 2
        assert blobs[2]["metric_config"]["resolution_error"] is None
        assert configs.as_of.call_count == 2
        assert resolve.call_count == 1

    def test_as_of_failure_for_one_member_only(self, query, resolve):
        configs = make_fake_configs(commit_hash_for_as_of="c1")  # not shared
        real_as_of = configs.as_of.side_effect
        calls = []

        def as_of(timestamp):
            calls.append(timestamp)
            if len(calls) == 2:
                raise RuntimeError("boom")
            return real_as_of(timestamp)

        configs.as_of.side_effect = as_of

        blobs = get_rows(query, ended_experiments("slug-a", "slug-b"), configs)

        assert blobs[0]["metric_config"]["resolution_error"] is None
        assert blobs[1]["metric_config"]["resolution_error"] == (
            f"{query.AS_OF_ERROR_PREFIX}boom"
        )

    def test_reuses_valid_previous_rows(self, query):
        configs = make_fake_configs()
        previous = {
            "slug-a": previous_row(
                query,
                config_as_of=FIRST_UPDATED_BY_SLUG["slug_a"],
                blob={"normandy_slug": "slug-a", "kept": 1},
            )
        }

        blobs = get_rows(query, ended_experiments("slug-a"), configs, previous=previous)

        configs.as_of.assert_not_called()
        assert blobs == [{"normandy_slug": "slug-a", "kept": 1}]


# An outcome whose statistics fail to parse, as in metric-config-parser's own tests.
BROKEN_OUTCOME = dedent("""
    friendly_name = "Performance outcomes"
    description = "Outcomes related to performance"

    [metrics]
    weekly = ["speed"]
    overall = ["speed"]

    [metrics.speed]
    data_source = "main"
    select_expression = "1"

    [metrics.speed.statistics.bootstrap_mean]
    """)


@pytest.fixture
def git_configs(tmp_path):
    """A metric-hub-like git repo: c1 is valid, c2 breaks parsing, c3 fixes it."""
    repo = Repo.init(tmp_path)
    for section, name, value in [
        ("user", "name", "test"),
        ("user", "email", "test@example.com"),
        ("commit", "gpgsign", "false"),
    ]:
        repo.config_writer().set_value(section, name, value).release()

    config_dir = tmp_path / "jetstream"
    outcome_dir = config_dir / "outcomes" / "firefox_desktop"
    outcome_dir.mkdir(parents=True)
    shas = []

    def commit(message, when):
        repo.git.add(".")
        with repo.git.custom_environment(GIT_COMMITTER_DATE=when, GIT_AUTHOR_DATE=when):
            repo.git.commit("-m", message)
        shas.append(repo.head.commit.hexsha)

    (config_dir / "slug-a.toml").write_text("[experiment]\nenrollment_period = 7\n")
    commit("c1", "2026-01-01T00:00:00+0000")
    (outcome_dir / "performance.toml").write_text(BROKEN_OUTCOME)
    commit("c2", "2026-02-01T00:00:00+0000")
    (outcome_dir / "performance.toml").unlink()
    commit("c3", "2026-03-01T00:00:00+0000")

    return ConfigCollection.from_github_repo(config_dir), shas


class TestGetMetricConfigsWithGitHistory:
    """Checks the predicted commits against the real ConfigCollection.as_of."""

    def run(self, query, configs, first_updated):
        """Return the rows and, per slug, the commit each was resolved with."""
        commits_seen = {}

        def resolve(nimbus_experiment, experiment_configs):
            commits_seen[nimbus_experiment.slug] = [
                repo.commit_hash for repo in experiment_configs.repos
            ]
            return query.MetricConfig()

        configs.as_of = MagicMock(wraps=configs.as_of)
        experiments = ended_experiments(*[s.replace("_", "-") for s in first_updated])
        with patch.object(query, "resolve_metric_config", side_effect=resolve):
            blobs = get_rows(query, experiments, configs, first_updated)
        return blobs, commits_seen

    def test_resolves_as_of_the_commit_before_first_updated(self, query, git_configs):
        configs, (c1, _, c3) = git_configs
        first_updated = {
            "slug_x": datetime.datetime(2026, 1, 15, tzinfo=UTC),
            "slug_z": datetime.datetime(2026, 1, 20, tzinfo=UTC),
            "slug_y": datetime.datetime(2026, 3, 15, tzinfo=UTC),
            # before the first commit, so the oldest commit is used
            "slug_w": datetime.datetime(2025, 12, 1, tzinfo=UTC),
        }

        blobs, commits_seen = self.run(query, configs, first_updated)

        assert commits_seen == {
            "slug-x": [c1],
            "slug-z": [c1],
            "slug-y": [c3],
            "slug-w": [c1],
        }
        assert configs.as_of.call_count == 2
        assert [b["normandy_slug"] for b in blobs] == [
            "slug-x",
            "slug-z",
            "slug-y",
            "slug-w",
        ]
        assert blobs[0]["config_as_of"] == first_updated["slug_x"].isoformat()
        assert blobs[0]["computed_at"] == NOW.isoformat()
        assert blobs[0]["resolver_fingerprint"] == FINGERPRINT

    def test_shares_the_result_when_a_commit_fails_to_parse(self, query, git_configs):
        configs, (_, _, c3) = git_configs
        first_updated = {
            "slug_x": datetime.datetime(2026, 2, 10, tzinfo=UTC),
            "slug_y": datetime.datetime(2026, 2, 20, tzinfo=UTC),
        }

        _, commits_seen = self.run(query, configs, first_updated)

        # both predicted c2, which fails to parse, so as_of moves on to c3
        assert commits_seen == {"slug-x": [c3], "slug-y": [c3]}
        assert configs.as_of.call_count == 1
