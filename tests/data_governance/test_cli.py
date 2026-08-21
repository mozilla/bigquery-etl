import datetime
from pathlib import Path

import pytest
from click.testing import CliRunner

# Import the bqetl CLI package before the group under test. The data_governance
# CLI takes one option from bigquery_etl.cli.utils, and bigquery_etl.cli imports
# the group back, so importing the group first finds it partially initialized.
import bigquery_etl.cli  # noqa: F401
from bigquery_etl.data_governance import cli
from bigquery_etl.data_governance.classification.config import (
    DEFAULT_DATASET,
    DEFAULT_MODEL,
    DEFAULT_PROJECT,
    DEFAULT_VERTEX_LOCATION,
)
from bigquery_etl.util import common

DATE = "2026-08-12"

TARGET = "src-proj.src_dataset"


@pytest.fixture(autouse=True)
def not_a_coding_agent(monkeypatch):
    """Switch off the guard that refuses to run under a coding agent.

    It reads the ambient environment, so without this the whole file passes or
    fails depending on which terminal it runs in.
    """
    monkeypatch.setattr(common, "is_running_under_coding_agent", lambda: False)


@pytest.fixture
def calls(monkeypatch):
    """Record what each library function was called with instead of calling it."""
    recorded = {}

    def record(name):
        def fake(*args, **kwargs):
            recorded[name] = (args, kwargs)

        return fake

    monkeypatch.setattr(cli.upstream, "profile", record("profile"))
    monkeypatch.setattr(cli.upstream, "resolve_lineage", record("lineage"))
    monkeypatch.setattr(cli.upstream, "fetch_probes", record("probes"))
    monkeypatch.setattr(cli.runner, "run", record("classify"))
    return recorded


def invoke(command, args):
    """Run one command and fail the test if it exited non-zero."""
    result = CliRunner().invoke(command, args, catch_exceptions=False)
    assert result.exit_code == 0, result.output
    return result


def profile_args(calls):
    """The (config, targets, date) positional arguments profile was called with."""
    return calls["profile"][0]


def test_target_without_table(calls):
    invoke(cli.profile, ["--date", DATE, "src-proj.src_dataset"])
    assert profile_args(calls)[1] == [("src-proj", "src_dataset", None)]


def test_target_with_table(calls):
    invoke(cli.profile, ["--date", DATE, "src-proj.src_dataset.my_table"])
    assert profile_args(calls)[1] == [("src-proj", "src_dataset", "my_table")]


def test_targets_keep_their_order(calls):
    invoke(cli.profile, ["--date", DATE, "p.b", "p.a", "p.c.t"])
    assert profile_args(calls)[1] == [
        ("p", "b", None),
        ("p", "a", None),
        ("p", "c", "t"),
    ]


@pytest.mark.parametrize("target", ["src_dataset", "a.b.c.d", "src proj.dataset"])
def test_invalid_target_rejected(calls, target):
    result = CliRunner().invoke(cli.profile, ["--date", DATE, target])
    assert result.exit_code != 0
    assert calls == {}


def test_target_without_a_project_says_so(calls):
    result = CliRunner().invoke(cli.profile, ["--date", DATE, "src_dataset"])
    # Rewrapped, because click breaks the message across the terminal width.
    assert "project is required" in " ".join(result.output.split())


def test_missing_target_rejected(calls):
    result = CliRunner().invoke(cli.profile, ["--date", DATE])
    assert result.exit_code != 0
    assert calls == {}


def test_date_reaches_the_library_as_a_date(calls):
    invoke(cli.profile, ["--date", DATE, TARGET])
    assert profile_args(calls)[2] == datetime.date(2026, 8, 12)


def test_profile_and_lineage_agree_on_the_date(calls):
    invoke(cli.profile, ["--date", DATE, TARGET])
    invoke(cli.lineage, ["--date", DATE])
    assert profile_args(calls)[2] == calls["lineage"][0][1]


@pytest.mark.parametrize(
    "command,args",
    [(cli.profile, [TARGET]), (cli.lineage, []), (cli.probes, [])],
)
def test_date_is_required(calls, command, args):
    result = CliRunner().invoke(command, args)
    assert result.exit_code != 0
    assert calls == {}


def test_config_defaults(calls):
    invoke(cli.classify, [TARGET])
    config = calls["classify"][0][0]
    assert config.project == DEFAULT_PROJECT
    assert config.dataset == DEFAULT_DATASET
    assert config.model == DEFAULT_MODEL
    assert config.vertex_location == DEFAULT_VERTEX_LOCATION
    assert config.sanitize is True
    assert config.vertex_project is None
    assert config.dlp_project is None
    assert config.dlp_quota_project is None


def test_config_flags_override_their_fields(calls):
    invoke(
        cli.classify,
        [
            "--project",
            "other-proj",
            "--dataset",
            "other_dataset",
            "--model",
            "gemini-other",
            "--vertex-location",
            "us-central1",
            "--vertex-project",
            "vertex-proj",
            "--dlp-project",
            "dlp-proj",
            "--dlp-quota-project",
            "quota-proj",
            TARGET,
        ],
    )
    config = calls["classify"][0][0]
    assert config.project == "other-proj"
    assert config.dataset == "other_dataset"
    assert config.model == "gemini-other"
    assert config.vertex_location == "us-central1"
    assert config.vertex_project == "vertex-proj"
    assert config.dlp_project == "dlp-proj"
    assert config.dlp_quota_project == "quota-proj"


def test_no_sanitize_clears_the_field(calls):
    invoke(cli.classify, ["--no-sanitize", TARGET])
    assert calls["classify"][0][0].sanitize is False


def test_invalid_config_value_is_a_bad_parameter(calls):
    result = CliRunner().invoke(cli.classify, ["--model", "gpt-4", TARGET])
    assert result.exit_code != 0
    assert "gpt-4" in result.output
    assert calls == {}


def test_run_id_is_used_verbatim(calls):
    invoke(cli.classify, ["--run-id", "given-id", TARGET])
    assert calls["classify"][0][0].run_id == "given-id"


def test_run_id_is_generated_when_omitted(calls):
    invoke(cli.classify, [TARGET])
    assert calls["classify"][0][0].run_id.strip()


def test_optional_pass_throughs_stay_none(calls):
    invoke(cli.profile, ["--date", DATE, TARGET])
    kwargs = calls["profile"][1]
    assert kwargs["max_workers"] is None
    assert kwargs["temp_dataset"] is None
    assert kwargs["dry_run"] is False


def test_optional_pass_throughs_are_forwarded(calls):
    invoke(
        cli.profile,
        [
            "--date",
            DATE,
            "--max-workers",
            "5",
            "--temp-dataset",
            "other-proj.tmp",
            "--dry-run",
            TARGET,
        ],
    )
    kwargs = calls["profile"][1]
    assert kwargs["max_workers"] == 5
    assert kwargs["temp_dataset"] == "other-proj.tmp"
    assert kwargs["dry_run"] is True


def test_sql_dir_arrives_as_a_path(calls):
    invoke(cli.profile, ["--date", DATE, "--sql-dir", "sql", TARGET])
    assert calls["profile"][1]["sql_dir"] == Path("sql")


def test_lineage_calls_resolve_lineage(calls):
    invoke(cli.lineage, ["--date", DATE, "--project", "p", "--dataset", "d"])
    config, date = calls["lineage"][0]
    assert (config.project, config.dataset) == ("p", "d")
    assert date == datetime.date(2026, 8, 12)
    assert "probes" not in calls


def test_probes_calls_fetch_probes(calls):
    invoke(cli.probes, ["--date", DATE, "--project", "p", "--dataset", "d"])
    config, date = calls["probes"][0]
    assert (config.project, config.dataset) == ("p", "d")
    assert date == datetime.date(2026, 8, 12)
    assert "lineage" not in calls


def test_classify_refresh(calls):
    invoke(cli.classify, ["--refresh", TARGET])
    assert calls["classify"][1] == {"refresh": True}


def test_classify_without_refresh(calls):
    invoke(cli.classify, [TARGET])
    assert calls["classify"][1] == {"refresh": False}


def test_library_errors_propagate(monkeypatch):
    def boom(*args, **kwargs):
        raise RuntimeError("classification failed for a.b")

    monkeypatch.setattr(cli.runner, "run", boom)
    result = CliRunner().invoke(cli.classify, [TARGET])
    assert result.exit_code != 0
    assert isinstance(result.exception, RuntimeError)
    assert "classification failed for a.b" in str(result.exception)


@pytest.mark.parametrize(
    "command",
    [cli.data_governance, cli.profile, cli.lineage, cli.probes, cli.classify],
)
def test_help(command):
    result = CliRunner().invoke(command, ["--help"], catch_exceptions=False)
    assert result.exit_code == 0
