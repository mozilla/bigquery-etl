import datetime
import logging
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from google.api_core import exceptions as api_exceptions
from google.cloud import bigquery

from bigquery_etl.data_governance.classification import upstream
from bigquery_etl.data_governance.classification.config import ClassificationConfig

CONFIG = ClassificationConfig(run_id="run-1", project="proj", dataset="meta")

DATE = datetime.date(2026, 8, 12)

SQL_DIR = Path(__file__).parents[3] / "sql"

TEMP_DATASET = "proj.tmp"

DATASET_TARGET = ("src-proj", "src_dataset", None)
TABLE_TARGET = ("src-proj", "src_dataset", "my_table")
OTHER_TARGET = ("src-proj", "other_dataset", None)

SCRATCH = "proj.tmp.classification_profiles_src_dataset_20260812"
OTHER_SCRATCH = "proj.tmp.classification_profiles_other_dataset_20260812"

# What the profiler is asked to do for DATASET_TARGET.
PROFILER_FLAGS = {
    "--date": "2026-08-12",
    "--source-project": "src-proj",
    "--source-datasets": "src_dataset",
    "--destination-project": "proj",
    "--destination-dataset": "tmp",
    "--destination-table": "classification_profiles_src_dataset_20260812",
}

# What get_table returns for the profiles table: the spec the scratch table is
# created from.
DESTINATION_SCHEMA = [
    bigquery.SchemaField("profiled_at", "DATE"),
    bigquery.SchemaField("source_dataset", "STRING"),
    bigquery.SchemaField("column_name", "STRING"),
]
DESTINATION_PARTITIONING = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.DAY, field="profiled_at"
)
DESTINATION_CLUSTERING = ["source_dataset", "source_table", "column_name"]

# (function, script it runs, its --source-table, its --destination-table)
UPSTREAM_STEPS = [
    (
        "resolve_lineage",
        "lineage_mapping_v1/query.py",
        "classification_column_profiles_v1",
        "classification_lineage_mapping_v1",
    ),
    (
        "fetch_probes",
        "probe_definitions_v1/query.py",
        "classification_lineage_mapping_v1",
        "classification_probe_definitions_v1",
    ),
]


def flags(command):
    """The script's flags as {flag: value}, with None for a flag taking no value.

    No flag's value starts with a dash, so the token after a flag is its value.
    """
    tokens = command[2:]
    return {
        token: (
            tokens[index + 1]
            if index + 1 < len(tokens) and not tokens[index + 1].startswith("--")
            else None
        )
        for index, token in enumerate(tokens)
        if token.startswith("--")
    }


class Fakes:
    """The BigQuery client and subprocess.run a pass sees, recording one sequence."""

    def __init__(self, scratch_rows=7, fail_datasets=(), fail_cleanup=False):
        self.scratch_rows = scratch_rows
        self.fail_datasets = set(fail_datasets)
        # Fails only the delete that follows the copy, not the one before the
        # create: both target the same scratch table.
        self.fail_cleanup = fail_cleanup
        # Every call in the order it was made, as (name, detail).
        self.calls = []
        self.clients = 0
        self.created = []
        self.job_configs = []
        self.copies = []

    # The client's surface, which is only what upstream.py calls.

    def get_table(self, table):
        self.calls.append(("get_table", table))
        if table == CONFIG.profiles_table:
            return SimpleNamespace(
                schema=DESTINATION_SCHEMA,
                time_partitioning=DESTINATION_PARTITIONING,
                clustering_fields=DESTINATION_CLUSTERING,
            )
        return SimpleNamespace(num_rows=self.scratch_rows)

    def create_table(self, table):
        self.calls.append(("create_table", str(table.reference)))
        self.created.append(table)

    def delete_table(self, table, not_found_ok=False):
        self.calls.append(("delete_table", table))
        copied = ("copy_table", (table, CONFIG.profiles_table))
        if self.fail_cleanup and copied in self.calls:
            raise api_exceptions.ServiceUnavailable("deadline exceeded")

    def copy_table(self, source, destination, job_config=None):
        self.calls.append(("copy_table", (source, destination)))
        self.copies.append((source, destination, job_config))
        return SimpleNamespace(result=lambda: None)

    def query(self, query, job_config=None):
        self.calls.append(("query", query))
        self.job_configs.append(job_config)
        return SimpleNamespace(result=lambda: [])

    def run(self, command, check=False):
        self.calls.append(("run", command))
        if flags(command).get("--source-datasets") in self.fail_datasets:
            raise subprocess.CalledProcessError(1, command)

    def _client_for(self, config):
        self.clients += 1
        return self

    def install(self, monkeypatch):
        monkeypatch.setattr(upstream, "client_for", self._client_for)
        monkeypatch.setattr(upstream.subprocess, "run", self.run)
        return self

    @property
    def names(self):
        """Just the call names, for asserting on the order of operations."""
        return [name for name, _detail in self.calls]

    @property
    def commands(self):
        """The argv of each script run, in order."""
        return [detail for name, detail in self.calls if name == "run"]


def profile(fakes, targets=(DATASET_TARGET,), **kwargs):
    """Profile the given targets with the fakes installed."""
    return upstream.profile(
        CONFIG,
        list(targets),
        DATE,
        sql_dir=SQL_DIR,
        temp_dataset=TEMP_DATASET,
        **kwargs,
    )


@pytest.fixture(autouse=True)
def datahub_token(monkeypatch):
    """A token by default, so only the tests about it are without one."""
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "a-token")


class TestProfilerArgv:
    def test_dataset_target_profiles_into_the_scratch_table(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        profile(fakes)

        assert flags(fakes.commands[0]) == PROFILER_FLAGS

    def test_table_target_adds_only_the_tables_flag(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        profile(fakes, targets=[TABLE_TARGET])

        assert flags(fakes.commands[0]) == {**PROFILER_FLAGS, "--tables": "my_table"}

    def test_max_workers_is_absent_unless_set(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        profile(fakes)

        assert "--max-workers" not in flags(fakes.commands[0])

    def test_max_workers_is_passed_through_when_set(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        profile(fakes, max_workers=5)

        assert flags(fakes.commands[0])["--max-workers"] == "5"


class TestScriptPath:
    def test_the_profiler_script_is_run_by_this_interpreter(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        profile(fakes)

        interpreter, script, *_args = fakes.commands[0]
        assert interpreter == sys.executable
        assert script.endswith(
            "sql/moz-fx-data-shared-prod/data_governance_metadata_derived"
            "/column_profiles_v1/query.py"
        )
        assert script.startswith(str(SQL_DIR))

    def test_a_missing_script_stops_the_pass_before_it_starts(
        self, monkeypatch, tmp_path
    ):
        fakes = Fakes().install(monkeypatch)

        with pytest.raises(FileNotFoundError) as excinfo:
            upstream.profile(
                CONFIG,
                [DATASET_TARGET],
                DATE,
                sql_dir=tmp_path,
                temp_dataset=TEMP_DATASET,
            )

        expected = (
            tmp_path
            / "moz-fx-data-shared-prod/data_governance_metadata_derived"
            / "column_profiles_v1/query.py"
        )
        assert str(expected.resolve()) in str(excinfo.value)
        # Nothing was created for a target that could never run.
        assert fakes.names == []


class TestScratchTable:
    def test_its_spec_comes_from_the_destination(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        profile(fakes)

        (created,) = fakes.created
        assert str(created.reference) == SCRATCH
        assert created.schema == DESTINATION_SCHEMA
        assert created.time_partitioning == DESTINATION_PARTITIONING
        assert created.clustering_fields == DESTINATION_CLUSTERING

    def test_it_expires(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        profile(fakes)

        (created,) = fakes.created
        assert created.expires > datetime.datetime.now(datetime.timezone.utc)

    def test_it_is_named_for_the_dataset_and_the_date(self):
        assert upstream._scratch_table(TEMP_DATASET, "src_dataset", DATE) == SCRATCH

    def test_the_destination_spec_is_read_once_for_the_whole_pass(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        profile(fakes, targets=[DATASET_TARGET, OTHER_TARGET])

        assert [detail for name, detail in fakes.calls if name == "get_table"] == [
            CONFIG.profiles_table,
            SCRATCH,
            OTHER_SCRATCH,
        ]
        assert fakes.clients == 1


class TestTempDataset:
    @pytest.mark.parametrize(
        "temp_dataset, message",
        [("tmp", "project.dataset"), ("proj.not a dataset", "temp_dataset dataset")],
    )
    def test_it_must_be_a_usable_project_and_dataset(
        self, monkeypatch, temp_dataset, message
    ):
        fakes = Fakes().install(monkeypatch)

        with pytest.raises(ValueError, match=message):
            upstream.profile(
                CONFIG,
                [DATASET_TARGET],
                DATE,
                sql_dir=SQL_DIR,
                temp_dataset=temp_dataset,
            )

        # Rejected once, before the first target rather than inside the loop.
        assert fakes.names == []


class TestOrderOfOperations:
    def test_the_scratch_table_is_filled_then_appended(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        profile(fakes)

        assert fakes.names == [
            # The destination's spec, then the scratch table it is copied to.
            "get_table",
            "delete_table",
            "create_table",
            "run",
            # Its row count, then the replace: delete, then copy.
            "get_table",
            "query",
            "copy_table",
            "delete_table",
        ]
        assert fakes.calls[-1] == ("delete_table", SCRATCH)

    def test_a_failed_target_leaves_its_scratch_table(self, monkeypatch):
        fakes = Fakes(fail_datasets=["src_dataset"]).install(monkeypatch)

        with pytest.raises(RuntimeError):
            profile(fakes)

        # Only the delete that precedes the create, so nothing cleaned it up.
        assert fakes.names == ["get_table", "delete_table", "create_table", "run"]

    def test_a_failed_cleanup_does_not_fail_the_target(self, monkeypatch, caplog):
        fakes = Fakes(fail_cleanup=True).install(monkeypatch)

        with caplog.at_level(logging.WARNING):
            summary = profile(fakes)

        # The rows were appended, so the target succeeded despite the leftover.
        assert (summary.targets, summary.rows, summary.failed) == (1, 7, [])
        assert SCRATCH in caplog.text


class TestDeletePredicate:
    def test_a_dataset_target_deletes_the_whole_dataset(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        profile(fakes)

        (job_config,) = fakes.job_configs
        parameters = {p.name: p.value for p in job_config.query_parameters}
        assert parameters == {
            "date": DATE,
            "project": "src-proj",
            "dataset": "src_dataset",
        }
        query = next(detail for name, detail in fakes.calls if name == "query")
        assert f"DELETE FROM `{CONFIG.profiles_table}`" in query
        assert "source_table" not in query

    def test_a_table_target_deletes_only_that_table(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        profile(fakes, targets=[TABLE_TARGET])

        (job_config,) = fakes.job_configs
        parameters = {p.name: p.value for p in job_config.query_parameters}
        assert parameters["table"] == "my_table"
        query = next(detail for name, detail in fakes.calls if name == "query")
        assert "AND source_table = @table" in query

    def test_the_partition_key_is_bound_as_a_date(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        profile(fakes)

        (job_config,) = fakes.job_configs
        date_parameter = next(
            p for p in job_config.query_parameters if p.name == "date"
        )
        assert date_parameter.type_ == "DATE"


class TestCopyJob:
    def test_the_scratch_rows_are_appended_to_the_profiles_table(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        summary = profile(fakes)

        source, destination, job_config = fakes.copies[0]
        assert source == SCRATCH
        assert destination == CONFIG.profiles_table
        assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_APPEND
        assert summary.rows == 7
        assert summary.targets == 1


class TestEmptyScratchTable:
    def test_it_leaves_the_destination_alone(self, monkeypatch):
        fakes = Fakes(scratch_rows=0).install(monkeypatch)

        summary = profile(fakes)

        assert fakes.names == [
            "get_table",
            "delete_table",
            "create_table",
            "run",
            "get_table",
            "delete_table",
        ]
        assert fakes.copies == []
        assert summary.rows == 0
        assert summary.targets == 1


class TestFailingTarget:
    def test_the_pass_continues_and_raises_naming_it(self, monkeypatch):
        fakes = Fakes(fail_datasets=["src_dataset"]).install(monkeypatch)

        with pytest.raises(RuntimeError) as excinfo:
            profile(fakes, targets=[DATASET_TARGET, OTHER_TARGET])

        # The message is built from summary.failed, which the raise keeps the
        # caller from reading.
        assert str(excinfo.value) == "Profiling failed for src-proj.src_dataset"
        # The second target was profiled and appended anyway.
        assert [flags(c)["--source-datasets"] for c in fakes.commands] == [
            "src_dataset",
            "other_dataset",
        ]
        assert [source for source, _dest, _config in fakes.copies] == [OTHER_SCRATCH]


class TestDryRun:
    def test_it_only_runs_the_profiler(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        summary = profile(fakes, dry_run=True)

        assert fakes.names == ["run"]
        assert flags(fakes.commands[0])["--dry-run"] is None
        assert summary.rows == 0
        assert summary.targets == 1

    def test_it_does_not_even_build_a_client(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        profile(fakes, dry_run=True)

        assert fakes.clients == 0


class TestLineageAndProbes:
    @pytest.mark.parametrize(
        "step, script, source_table, destination_table", UPSTREAM_STEPS
    )
    def test_each_step_reads_and_writes_our_tables(
        self, monkeypatch, step, script, source_table, destination_table
    ):
        fakes = Fakes().install(monkeypatch)

        getattr(upstream, step)(CONFIG, DATE, sql_dir=SQL_DIR)

        command = fakes.commands[0]
        assert command[1].endswith(script)
        assert flags(command) == {
            "--date": "2026-08-12",
            "--source-project": "proj",
            "--source-dataset": "meta",
            "--source-table": source_table,
            "--destination-project": "proj",
            "--destination-dataset": "meta",
            "--destination-table": destination_table,
        }

    def test_neither_touches_bigquery_itself(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        upstream.resolve_lineage(CONFIG, DATE, sql_dir=SQL_DIR)
        upstream.fetch_probes(CONFIG, DATE, sql_dir=SQL_DIR)

        assert fakes.names == ["run", "run"]

    def test_max_workers_is_passed_through(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        upstream.fetch_probes(CONFIG, DATE, sql_dir=SQL_DIR, max_workers=4)

        assert flags(fakes.commands[0])["--max-workers"] == "4"

    def test_dry_run_is_passed_through(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        upstream.resolve_lineage(CONFIG, DATE, sql_dir=SQL_DIR, dry_run=True)

        assert flags(fakes.commands[0])["--dry-run"] is None

    def test_every_step_agrees_on_the_date(self, monkeypatch):
        fakes = Fakes().install(monkeypatch)

        profile(fakes)
        upstream.resolve_lineage(CONFIG, DATE, sql_dir=SQL_DIR)
        upstream.fetch_probes(CONFIG, DATE, sql_dir=SQL_DIR)

        assert {flags(command)["--date"] for command in fakes.commands} == {
            "2026-08-12"
        }


class TestDatahubToken:
    @pytest.mark.parametrize("step", ["resolve_lineage", "fetch_probes"])
    def test_it_is_required_before_spawning(self, monkeypatch, step):
        monkeypatch.delenv("DATAHUB_GMS_TOKEN")
        fakes = Fakes().install(monkeypatch)

        with pytest.raises(RuntimeError, match="DATAHUB_GMS_TOKEN"):
            getattr(upstream, step)(CONFIG, DATE, sql_dir=SQL_DIR)

        assert fakes.commands == []

    def test_profiling_does_not_need_it(self, monkeypatch):
        monkeypatch.delenv("DATAHUB_GMS_TOKEN")
        fakes = Fakes().install(monkeypatch)

        summary = profile(fakes)

        assert summary.rows == 7
