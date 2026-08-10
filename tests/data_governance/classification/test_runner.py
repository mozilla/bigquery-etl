import logging
import re
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from bigquery_etl.data_governance.classification import runner
from bigquery_etl.data_governance.classification.config import ClassificationConfig
from bigquery_etl.data_governance.classification.models import TokenTotals
from bigquery_etl.data_governance.classification.runner import (
    ModelUnavailableError,
    RunSummary,
    _answer_fields,
)
from bigquery_etl.data_governance.classification.sanitize import (
    DROP,
    KEEP,
    MASK,
    Sanitizer,
    SanitizeResult,
)

CONFIG = ClassificationConfig(run_id="run-1", project="proj", dataset="meta")
NO_SANITIZE_CONFIG = replace(CONFIG, sanitize=False)

TARGET = ("src-proj", "src_dataset", None)
TABLE_KEY = ("src-proj", "src_dataset", "my_table")

PING = {"source_ping": "fenix.baseline", "ping_platform": "glean"}
PING_KEY = ("glean", "fenix.baseline")

KNOWN_LABEL = "user.account"
OTHER_KNOWN_LABEL = "user.behavior"

SCHEMA_PATH = (
    Path(__file__).parents[3]
    / "sql/moz-fx-data-shared-prod/data_governance_metadata_derived"
    / "column_classifications_v1/schema.yaml"
)


def schema_field_names():
    """Top-level field names of the deployed destination schema."""
    fields = yaml.safe_load(SCHEMA_PATH.read_text())["fields"]
    return {f["name"] for f in fields}


COLUMN_DEFAULTS = {
    "column_name": "submission_timestamp",
    "data_type": "TIMESTAMP",
    "bq_description": "when it arrived",
    "is_glean_metric": False,
    "column_tier": "scalar",
    "null_rate": 1.5,
    "distinct_count": 3,
    "is_high_cardinality": False,
    "example_value": "abc",
    "values": [("abc", 3), ("def", 1)],
}

# A Glean metric column is in the schema but never profiled, so it arrives with
# the statistics all None.
METRIC_DEFAULTS: dict[str, Any] = {
    "column_name": "metrics.string.account_user_id",
    "data_type": "STRING",
    "bq_description": None,
    "is_glean_metric": True,
    "column_tier": None,
    "null_rate": None,
    "distinct_count": None,
    "is_high_cardinality": None,
    "example_value": None,
    "values": [],
}

PROBE_DEFAULTS = {
    "probe_name": "account.user_id",
    "probe_description": "The account id.",
    "probe_type": "string",
    "data_sensitivity": ["highly_sensitive"],
    "tags": ["Account"],
}

ANSWER_DEFAULTS = {
    "primary_label": KNOWN_LABEL,
    "secondary_labels": [],
    "confidence": "high",
    "reasoning": "because",
    "needs_review": False,
}


def column(**overrides):
    """A column dict in the shape load_columns hands over."""
    return {**COLUMN_DEFAULTS, **overrides}


def metric_column(name=METRIC_DEFAULTS["column_name"], **overrides):
    """A Glean metric column, keyed by its metrics.<type>.<name> path."""
    return {**METRIC_DEFAULTS, "column_name": name, **overrides}


def probe(name=PROBE_DEFAULTS["probe_name"], **overrides):
    """A probe dict in the shape load_probes_by_ping hands over."""
    return {**PROBE_DEFAULTS, "probe_name": name, **overrides}


def answer(**overrides):
    """A well-formed model answer."""
    return {**ANSWER_DEFAULTS, **overrides}


class FakeModel:
    """Stands in for VertexModel: canned answers in order, running totals."""

    def __init__(self, answers=None, default=None):
        self.answers = list(answers or [])
        self.default = default
        self.prompts = []
        self.totals = TokenTotals()

    def generate_json(self, prompt):
        self.prompts.append(prompt)
        if self.answers:
            result = self.answers.pop(0)
        elif self.default is not None:
            result = dict(self.default)
        else:
            raise AssertionError(f"unexpected model call for prompt: {prompt[:80]}")
        self.totals.calls += 1
        if isinstance(result, Exception):
            raise result
        return result


class FakeBigQueryClient:
    """Records the queries and load jobs it is given, in order."""

    def __init__(self):
        self.calls = []
        self.queries = []
        self.job_configs = []
        self.loads = []
        # Set to an exception to make every load job fail.
        self.load_error = None

    def query(self, query, job_config=None):
        self.calls.append("query")
        self.queries.append(query)
        self.job_configs.append(job_config)
        return SimpleNamespace(result=lambda: [])

    def load_table_from_json(self, records, destination, job_config=None):
        self.calls.append("load")
        self.loads.append((list(records), destination, job_config))
        if self.load_error is not None:
            raise self.load_error
        return SimpleNamespace(result=lambda: None)


class FakeSanitizer:
    """Stands in for Sanitizer; the real sanitize_columns runs against it."""

    def __init__(self, results=None):
        # {value: SanitizeResult}; anything else is kept as-is.
        self.results = results or {}
        self.asked = []

    def sanitize(self, value):
        self.asked.append(value)
        return self.results.get(value) or SanitizeResult(KEEP, value, [])

    # Borrowed rather than reimplemented, so the dedup under test is production's.
    sanitize_many = Sanitizer.sanitize_many


class Harness:
    """The module's collaborators, all faked, with the calls they received."""

    def __init__(
        self,
        columns=None,
        answers=None,
        default_answer=None,
        ping_mapping=None,
        probes_by_ping=None,
        already=None,
        config=CONFIG,
        sanitizer=None,
    ):
        self.config = config
        self.client = FakeBigQueryClient()
        self.model = FakeModel(answers, default=default_answer)
        self.sanitizer = sanitizer or FakeSanitizer()
        self.default_columns = columns if columns is not None else {}
        self.columns_by_target = {}
        self.ping_mapping = ping_mapping or {}
        self.probes_by_ping = probes_by_ping or {}
        self.already = already or set()
        self.load_columns_calls = []
        self.already_calls = []
        self.sanitizer_calls = 0
        # Set by install()'s RunSummary factory, so a run that raises can still
        # be asserted against.
        self.summary = None

    def install(self, monkeypatch):
        monkeypatch.setattr(runner, "RunSummary", self._new_summary)
        monkeypatch.setattr(runner, "client_for", lambda config: self.client)
        monkeypatch.setattr(runner, "model_for", lambda config: self.model)
        monkeypatch.setattr(runner, "sanitizer_for", self._sanitizer_for)
        monkeypatch.setattr(runner, "load_columns", self._load_columns)
        monkeypatch.setattr(
            runner, "load_ping_mapping", lambda client, config: self.ping_mapping
        )
        monkeypatch.setattr(
            runner, "load_probes_by_ping", lambda client, config: self.probes_by_ping
        )
        monkeypatch.setattr(runner, "load_already_classified", self._load_already)
        return self

    def _new_summary(self):
        self.summary = RunSummary()
        return self.summary

    def _sanitizer_for(self, config):
        self.sanitizer_calls += 1
        # Mirrors the real factory: None means sanitization is disabled.
        return self.sanitizer if config.sanitize else None

    def _load_columns(self, client, config, project, dataset, table=None):
        self.load_columns_calls.append((project, dataset, table))
        result = self.columns_by_target.get((project, dataset), self.default_columns)
        if isinstance(result, Exception):
            raise result
        return result

    def _load_already(self, client, config, project, dataset, table=None):
        self.already_calls.append((project, dataset, table))
        return set(self.already)

    def run(self, targets=None, **kwargs):
        return runner.run(self.config, targets or [TARGET], **kwargs)

    @property
    def records(self):
        """Every record loaded, across batches."""
        return [
            record
            for records, _dest, _config in self.client.loads
            for record in records
        ]

    def deleted_columns(self):
        """The @columns array of each DELETE, in order."""
        arrays = []
        for job_config in self.client.job_configs:
            for parameter in job_config.query_parameters:
                if parameter.name == "columns":
                    arrays.append(list(parameter.values))
        return arrays


def one_table(columns):
    """A single-table load_columns result."""
    return {TABLE_KEY: columns}


class TestHappyPath:
    def test_every_field_is_populated_from_the_run(self, monkeypatch):
        harness = Harness(
            columns=one_table([column(), column(column_name="user_pref")]),
            answers=[answer(), answer(primary_label=OTHER_KNOWN_LABEL)],
        ).install(monkeypatch)

        summary = harness.run()

        assert summary.tables == 1
        assert summary.classified == 2
        assert summary.skipped == 0
        assert [r["column_name"] for r in harness.records] == [
            "submission_timestamp",
            "user_pref",
        ]
        record = harness.records[0]
        assert record["run_id"] == "run-1"
        assert record["source_project"] == "src-proj"
        assert record["source_dataset"] == "src_dataset"
        assert record["source_table"] == "my_table"
        assert record["data_type"] == "TIMESTAMP"
        assert record["primary_label"] == KNOWN_LABEL
        assert record["secondary_labels"] == []
        assert record["confidence"] == "high"
        assert record["reasoning"] == "because"
        assert record["needs_review"] is False
        assert record["matched_probe"] is None
        assert record["data_sensitivity"] == []
        assert record["model"] == CONFIG.model
        assert record["taxonomy_version"] == "v1"
        assert record["classified_at"]
        assert "input_hash" not in record

    def test_record_keys_are_destination_columns(self, monkeypatch):
        harness = Harness(columns=one_table([column()]), answers=[answer()]).install(
            monkeypatch
        )

        harness.run()

        names = schema_field_names()
        assert set(harness.records[0]) <= names
        # Nothing but input_hash is left out.
        assert names - set(harness.records[0]) == {"input_hash"}

    def test_prompt_names_the_table_without_the_project(self, monkeypatch):
        harness = Harness(columns=one_table([column()]), answers=[answer()]).install(
            monkeypatch
        )

        harness.run()

        assert "Table: src_dataset.my_table" in harness.model.prompts[0]


class TestSkipping:
    def test_classified_column_is_not_sent_to_the_model(self, monkeypatch):
        harness = Harness(
            columns=one_table([column(), column(column_name="user_pref")]),
            answers=[answer()],
            already={(*TABLE_KEY, "submission_timestamp")},
        ).install(monkeypatch)

        summary = harness.run()

        assert summary.skipped == 1
        assert summary.classified == 1
        assert [r["column_name"] for r in harness.records] == ["user_pref"]

    def test_fully_classified_table_costs_nothing(self, monkeypatch):
        harness = Harness(
            columns=one_table([column()]),
            already={(*TABLE_KEY, "submission_timestamp")},
        ).install(monkeypatch)

        summary = harness.run()

        assert summary.skipped == 1
        assert summary.classified == 0
        assert harness.model.prompts == []
        assert harness.client.calls == []
        assert harness.sanitizer.asked == []

    def test_refresh_reclassifies_without_reading_what_is_done(self, monkeypatch):
        harness = Harness(
            columns=one_table([column()]),
            answers=[answer()],
            already={(*TABLE_KEY, "submission_timestamp")},
        ).install(monkeypatch)

        summary = harness.run(refresh=True)

        assert harness.already_calls == []
        assert summary.skipped == 0
        assert [r["column_name"] for r in harness.records] == ["submission_timestamp"]


class TestMetricGating:
    def test_ordinary_column_without_a_probe_is_still_classified(self, monkeypatch):
        harness = Harness(
            columns=one_table([column()]),
            answers=[answer()],
            ping_mapping={TABLE_KEY: PING},
            probes_by_ping={PING_KEY: [probe()]},
        ).install(monkeypatch)

        summary = harness.run()

        assert summary.classified == 1
        assert summary.dropped_metrics == 0
        assert harness.records[0]["matched_probe"] is None
        assert "Candidate probes: none matched." in harness.model.prompts[0]

    def test_metric_leaf_takes_its_probe(self, monkeypatch):
        harness = Harness(
            columns=one_table([metric_column()]),
            answers=[answer()],
            ping_mapping={TABLE_KEY: PING},
            probes_by_ping={PING_KEY: [probe()]},
        ).install(monkeypatch)

        summary = harness.run()

        assert summary.classified == 1
        record = harness.records[0]
        assert record["matched_probe"] == "account.user_id"
        assert record["data_sensitivity"] == ["highly_sensitive"]

    def test_metric_leaf_without_an_exact_match_is_dropped(self, monkeypatch):
        harness = Harness(
            columns=one_table([metric_column("metrics.counter.something_else")]),
            ping_mapping={TABLE_KEY: PING},
            probes_by_ping={PING_KEY: [probe()]},
        ).install(monkeypatch)

        summary = harness.run()

        assert summary.dropped_metrics == 1
        assert summary.classified == 0
        assert harness.model.prompts == []
        assert harness.client.calls == []

    def test_unresolved_table_drops_metrics_and_keeps_the_rest(self, monkeypatch):
        harness = Harness(
            columns=one_table([column(), metric_column()]),
            answers=[answer()],
        ).install(monkeypatch)

        summary = harness.run()

        assert summary.dropped_metrics == 1
        assert summary.classified == 1
        assert [r["column_name"] for r in harness.records] == ["submission_timestamp"]


class TestPingResolution:
    def test_unresolved_ping_is_not_looked_up(self, monkeypatch):
        harness = Harness(
            columns=one_table([column()]),
            answers=[answer()],
            ping_mapping={TABLE_KEY: {"source_ping": None, "ping_platform": None}},
            # Would be found if the runner keyed the lookup on (None, None).
            probes_by_ping={(None, None): [probe()]},
        ).install(monkeypatch)

        summary = harness.run()

        assert summary.classified == 1
        assert harness.records[0]["matched_probe"] is None
        assert "Candidate probes: none matched." in harness.model.prompts[0]


class TestSanitization:
    def test_high_cardinality_samples_never_reach_dlp(self, monkeypatch):
        harness = Harness(
            columns=one_table([column(is_high_cardinality=True, distinct_count=9999)]),
            answers=[answer()],
        ).install(monkeypatch)

        harness.run()

        assert harness.sanitizer.asked == []
        assert harness.records[0]["evidence"]["sanitized_samples"] == []

    def test_dlp_result_is_what_gets_recorded(self, monkeypatch):
        sanitizer = FakeSanitizer(
            {
                "abc": SanitizeResult(MASK, "[EMAIL_ADDRESS]", ["EMAIL_ADDRESS"]),
                "def": SanitizeResult(DROP, None, ["CREDIT_CARD_NUMBER"]),
            }
        )
        harness = Harness(
            columns=one_table([column()]),
            answers=[answer()],
            sanitizer=sanitizer,
        ).install(monkeypatch)

        harness.run()

        assert sanitizer.asked
        assert harness.records[0]["evidence"]["sanitized_samples"] == [
            "[EMAIL_ADDRESS]"
        ]

    def test_an_unsanitized_run_keeps_no_samples(self, monkeypatch):
        harness = Harness(
            columns=one_table([column()]),
            answers=[answer()],
            config=NO_SANITIZE_CONFIG,
        ).install(monkeypatch)

        harness.run()

        # The model saw the values; the table does not keep them.
        assert "Top values (value: frequency): 'abc' (3)" in harness.model.prompts[0]
        assert harness.records[0]["evidence"]["sanitized_samples"] == []

    def test_disabled_sanitizer_still_suppresses(self, monkeypatch):
        harness = Harness(
            columns=one_table([column(is_high_cardinality=True, distinct_count=9999)]),
            answers=[answer()],
            config=NO_SANITIZE_CONFIG,
        ).install(monkeypatch)

        harness.run()

        assert harness.sanitizer.asked == []
        assert harness.records[0]["evidence"]["sanitized_samples"] == []
        assert "Top values" not in harness.model.prompts[0]


class TestEvidence:
    def test_mirrors_the_column(self, monkeypatch):
        harness = Harness(columns=one_table([column()]), answers=[answer()]).install(
            monkeypatch
        )

        harness.run()

        assert harness.records[0]["evidence"] == {
            "null_rate": 1.5,
            "distinct_count": 3,
            "is_high_cardinality": False,
            "column_tier": "scalar",
            "existing_description": "when it arrived",
            "sanitized_samples": ["abc", "def"],
        }

    def test_metric_leaf_records_no_samples(self, monkeypatch):
        harness = Harness(
            # A metric leaf carries no values; even if it somehow did, the
            # prompt renders none, so none are recorded.
            columns=one_table([metric_column(values=[("abc", 3)])]),
            answers=[answer()],
            ping_mapping={TABLE_KEY: PING},
            probes_by_ping={PING_KEY: [probe()]},
        ).install(monkeypatch)

        harness.run()

        evidence = harness.records[0]["evidence"]
        assert evidence["sanitized_samples"] == []
        assert evidence["null_rate"] is None
        assert evidence["distinct_count"] is None
        assert evidence["column_tier"] is None

    def test_missing_description_is_none(self, monkeypatch):
        harness = Harness(
            columns=one_table([column(bq_description=None)]), answers=[answer()]
        ).install(monkeypatch)

        harness.run()

        assert harness.records[0]["evidence"]["existing_description"] is None

    def test_example_value_stands_in_for_top_values(self, monkeypatch):
        harness = Harness(
            columns=one_table([column(values=[], example_value="only-one")]),
            answers=[answer()],
        ).install(monkeypatch)

        harness.run()

        assert harness.records[0]["evidence"]["sanitized_samples"] == ["only-one"]
        assert "Example value: only-one" in harness.model.prompts[0]

    def test_samples_are_exactly_what_the_prompt_rendered(self, monkeypatch):
        values = [(f"v{i}", 100 - i) for i in range(20)]
        harness = Harness(
            columns=one_table([column(values=values, distinct_count=20)]),
            answers=[answer()],
        ).install(monkeypatch)

        harness.run()

        (top_values_line,) = [
            line
            for line in harness.model.prompts[0].splitlines()
            if line.startswith("Top values")
        ]
        rendered = re.findall(r"'([^']*)' \(", top_values_line)
        samples = harness.records[0]["evidence"]["sanitized_samples"]
        assert samples == rendered
        assert len(samples) < len(values)


class TestAnswerFields:
    LABELS = frozenset({KNOWN_LABEL, OTHER_KNOWN_LABEL})

    def test_unknown_primary_label_is_kept_and_flagged(self):
        fields = _answer_fields(
            answer(primary_label="user.invented", needs_review=False),
            self.LABELS,
            "col",
        )

        assert fields["primary_label"] == "user.invented"
        assert fields["needs_review"] is True

    def test_unknown_secondary_label_is_kept_and_flagged(self):
        fields = _answer_fields(
            answer(secondary_labels=[OTHER_KNOWN_LABEL, "user.invented"]),
            self.LABELS,
            "col",
        )

        assert fields["secondary_labels"] == [OTHER_KNOWN_LABEL, "user.invented"]
        assert fields["needs_review"] is True

    @pytest.mark.parametrize("needs_review", [True, False, None])
    def test_known_labels_pass_needs_review_through(self, needs_review):
        fields = _answer_fields(answer(needs_review=needs_review), self.LABELS, "col")

        assert fields["needs_review"] is needs_review

    def test_only_a_label_is_required(self):
        # Nothing but primary_label is mandatory, so a terse answer is usable.
        fields = _answer_fields({"primary_label": KNOWN_LABEL}, self.LABELS, "col")

        assert fields["secondary_labels"] == []
        assert fields["confidence"] is None
        assert fields["reasoning"] is None
        assert fields["needs_review"] is None

    @pytest.mark.parametrize(
        "result",
        [
            ["user.account"],
            "user.account",
            None,
            answer(secondary_labels=OTHER_KNOWN_LABEL),
            answer(primary_label=["user.account"]),
            answer(needs_review="yes"),
            answer(primary_label=None),
            answer(primary_label=""),
            {"reasoning": "no idea"},
        ],
        ids=[
            "list",
            "string",
            "none",
            "secondary_labels_string",
            "primary_label_list",
            "needs_review_string",
            "primary_label_none",
            "primary_label_empty",
            "no_primary_label_key",
        ],
    )
    def test_unusable_answer_raises(self, result):
        with pytest.raises(ValueError):
            _answer_fields(result, self.LABELS, "col")


class TestFailureHandling:
    def test_failed_call_is_retried_and_succeeds(self, monkeypatch):
        harness = Harness(
            columns=one_table([column(), column(column_name="user_pref")]),
            # The sweep re-asks about the first column after the table's last one.
            answers=[RuntimeError("boom"), answer(), answer()],
        ).install(monkeypatch)

        summary = harness.run()

        assert summary.failed_columns == 0
        assert summary.failed_column_names == []
        assert summary.classified == 2
        assert sorted(r["column_name"] for r in harness.records) == [
            "submission_timestamp",
            "user_pref",
        ]

    def test_malformed_answer_is_retried_and_succeeds(self, monkeypatch):
        harness = Harness(
            columns=one_table([column()]),
            answers=[answer(secondary_labels="oops"), answer()],
        ).install(monkeypatch)

        summary = harness.run()

        assert summary.failed_columns == 0
        assert [r["column_name"] for r in harness.records] == ["submission_timestamp"]

    @pytest.mark.parametrize("label", [None, ""], ids=["none", "empty"])
    def test_an_answer_without_a_label_is_retried_then_fails(self, monkeypatch, label):
        harness = Harness(
            columns=one_table([column()]),
            answers=[answer(primary_label=label)] * 3,
        ).install(monkeypatch)

        with pytest.raises(RuntimeError, match="1 columns unclassified"):
            harness.run()

        assert len(harness.model.prompts) == 1 + runner.MAX_ANSWER_RETRIES
        assert harness.records == []
        assert harness.summary.classified == 0

    def test_permanent_failure_is_counted_once_and_named(self, monkeypatch):
        harness = Harness(
            columns=one_table([column()]),
            answers=[RuntimeError("boom")] * 3,
        ).install(monkeypatch)

        with pytest.raises(RuntimeError, match="1 columns unclassified"):
            harness.run()

        assert len(harness.model.prompts) == 1 + runner.MAX_ANSWER_RETRIES
        assert harness.summary.failed_columns == 1
        assert harness.summary.failed_column_names == [
            "src_dataset.my_table.submission_timestamp"
        ]

    def test_permanent_failure_reports_the_column_and_keeps_other_rows(
        self, monkeypatch
    ):
        harness = Harness(
            columns=one_table([column(), column(column_name="user_pref")]),
            answers=[RuntimeError("boom"), answer(), RuntimeError("boom")],
        ).install(monkeypatch)

        with pytest.raises(RuntimeError) as excinfo:
            harness.run()

        assert "src_dataset.my_table.submission_timestamp" in str(excinfo.value)
        # The column that succeeded is written before the run raises.
        assert [r["column_name"] for r in harness.records] == ["user_pref"]

    def test_the_sweep_reuses_the_first_prompt(self, monkeypatch):
        harness = Harness(
            columns=one_table([column()]),
            answers=[RuntimeError("boom"), answer()],
        ).install(monkeypatch)

        harness.run()

        # Each sample value is asked about once for the whole table, and the
        # retried column gets the prompt it already had.
        assert harness.sanitizer.asked == ["abc", "def"]
        assert harness.model.prompts[1] == harness.model.prompts[0]

    def test_consecutive_failures_abandon_the_run(self, monkeypatch):
        monkeypatch.setattr(runner, "MAX_CONSECUTIVE_FAILURES", 2)
        monkeypatch.setattr(runner, "WRITE_BATCH_SIZE", 1)
        harness = Harness(
            columns=one_table(
                [
                    column(column_name="a"),
                    column(column_name="b"),
                    column(column_name="c"),
                    column(column_name="d"),
                ]
            ),
            answers=[answer(), RuntimeError("boom"), RuntimeError("boom")],
        ).install(monkeypatch)

        with pytest.raises(ModelUnavailableError):
            harness.run()

        # The flushed batch stays written; the last column is never attempted.
        assert [r["column_name"] for r in harness.records] == ["a"]
        assert len(harness.model.prompts) == 3

    def test_a_success_resets_the_counter(self, monkeypatch):
        monkeypatch.setattr(runner, "MAX_CONSECUTIVE_FAILURES", 2)
        harness = Harness(
            columns=one_table(
                [
                    column(column_name="a"),
                    column(column_name="b"),
                    column(column_name="c"),
                ]
            ),
            # a and c fail with b succeeding between them, so the breaker never
            # sees two in a row; the sweep then classifies both.
            answers=[
                RuntimeError("boom"),
                answer(),
                RuntimeError("boom"),
                answer(),
                answer(),
            ],
        ).install(monkeypatch)

        summary = harness.run()

        assert summary.failed_columns == 0
        assert sorted(r["column_name"] for r in harness.records) == ["a", "b", "c"]

    def test_the_breaker_flushes_the_pending_batch(self, monkeypatch):
        monkeypatch.setattr(runner, "MAX_CONSECUTIVE_FAILURES", 2)
        monkeypatch.setattr(runner, "WRITE_BATCH_SIZE", 5)
        harness = Harness(
            columns=one_table([column(column_name=n) for n in "abcd"]),
            answers=[answer(), answer(), RuntimeError("boom"), RuntimeError("boom")],
        ).install(monkeypatch)

        with pytest.raises(ModelUnavailableError):
            harness.run()

        # Two records is short of WRITE_BATCH_SIZE, so only the flush on the way
        # out can have written them.
        assert [r["column_name"] for r in harness.records] == ["a", "b"]
        # The abandoned pass still reports the columns it failed on.
        assert harness.summary.failed_columns == 2
        assert harness.summary.failed_column_names == [
            "src_dataset.my_table.c",
            "src_dataset.my_table.d",
        ]

    def test_the_breaker_still_reports_what_the_run_did(self, monkeypatch, caplog):
        monkeypatch.setattr(runner, "MAX_CONSECUTIVE_FAILURES", 1)
        harness = Harness(
            columns=one_table([column()]), answers=[RuntimeError("boom")]
        ).install(monkeypatch)

        with caplog.at_level(logging.INFO):
            with pytest.raises(ModelUnavailableError):
                harness.run()

        assert any("columns classified" in m for m in caplog.messages)
        assert any(m.startswith("TOKENS ") for m in caplog.messages)

    def test_an_unpromptable_column_fails_only_itself(self, monkeypatch):
        harness = Harness(
            # A None frequency breaks the prompt's rendering of top values.
            columns=one_table(
                [column(values=[("abc", None)]), column(column_name="user_pref")]
            ),
            answers=[answer()],
        ).install(monkeypatch)

        with pytest.raises(RuntimeError, match="1 columns unclassified"):
            harness.run()

        # The dataset is not what failed, so its other columns are classified.
        assert harness.summary.failed_targets == []
        assert harness.summary.failed_column_names == [
            "src_dataset.my_table.submission_timestamp"
        ]
        assert [r["column_name"] for r in harness.records] == ["user_pref"]

    def test_unpromptable_columns_do_not_trip_the_breaker(self, monkeypatch):
        # The model is never called for these, so they say nothing about it.
        monkeypatch.setattr(runner, "MAX_CONSECUTIVE_FAILURES", 1)
        harness = Harness(
            columns=one_table(
                [column(column_name=name, values=[("abc", None)]) for name in "ab"]
            ),
        ).install(monkeypatch)

        with pytest.raises(RuntimeError, match="2 columns unclassified"):
            harness.run()

        assert harness.model.prompts == []

    def test_a_flush_failure_does_not_hide_the_breaker(self, monkeypatch):
        monkeypatch.setattr(runner, "MAX_CONSECUTIVE_FAILURES", 2)
        monkeypatch.setattr(runner, "WRITE_BATCH_SIZE", 5)
        harness = Harness(
            columns=one_table([column(column_name=n) for n in "abcd"]),
            answers=[answer(), answer(), RuntimeError("boom"), RuntimeError("boom")],
        ).install(monkeypatch)
        harness.client.load_error = RuntimeError("load failed")

        # The load error is logged, not raised in place of the breaker.
        with pytest.raises(ModelUnavailableError):
            harness.run()


class TestWrite:
    def test_delete_precedes_the_load_and_names_the_batch(self, monkeypatch):
        harness = Harness(
            columns=one_table([column(), column(column_name="user_pref")]),
            answers=[answer(), answer()],
        ).install(monkeypatch)

        harness.run()

        assert harness.client.calls == ["query", "load"]
        assert f"DELETE FROM `{CONFIG.destination_table}`" in harness.client.queries[0]
        parameters = {p.name: p for p in harness.client.job_configs[0].query_parameters}
        assert parameters["project"].value == "src-proj"
        assert parameters["dataset"].value == "src_dataset"
        assert parameters["table"].value == "my_table"
        assert list(parameters["columns"].values) == [
            "submission_timestamp",
            "user_pref",
        ]
        assert parameters["columns"].array_type == "STRING"

    def test_load_appends_against_the_deployed_schema(self, monkeypatch):
        harness = Harness(columns=one_table([column()]), answers=[answer()]).install(
            monkeypatch
        )

        harness.run()

        _records, destination, job_config = harness.client.loads[0]
        assert destination == CONFIG.destination_table
        assert job_config.write_disposition == "WRITE_APPEND"
        assert job_config.schema is None
        assert job_config.schema_update_options is None

    def test_batches_are_written_as_they_fill(self, monkeypatch):
        monkeypatch.setattr(runner, "WRITE_BATCH_SIZE", 2)
        harness = Harness(
            columns=one_table([column(column_name=n) for n in "abcde"]),
            default_answer=answer(),
        ).install(monkeypatch)

        harness.run()

        assert [len(records) for records, _d, _c in harness.client.loads] == [2, 2, 1]
        assert harness.deleted_columns() == [["a", "b"], ["c", "d"], ["e"]]
        assert harness.client.calls == ["query", "load"] * 3


class TestTargets:
    SECOND_TARGET = ("src-proj", "other_dataset", None)
    SECOND_KEY = ("src-proj", "other_dataset", "other_table")

    def test_both_targets_run(self, monkeypatch):
        harness = Harness(default_answer=answer()).install(monkeypatch)
        harness.columns_by_target = {
            ("src-proj", "src_dataset"): one_table([column(column_name="a")]),
            ("src-proj", "other_dataset"): {self.SECOND_KEY: [column(column_name="b")]},
        }

        summary = harness.run([TARGET, self.SECOND_TARGET])

        assert summary.tables == 2
        assert [r["source_dataset"] for r in harness.records] == [
            "src_dataset",
            "other_dataset",
        ]

    def test_failed_target_is_recorded_and_the_run_still_fails(self, monkeypatch):
        harness = Harness(default_answer=answer()).install(monkeypatch)
        harness.columns_by_target = {
            ("src-proj", "src_dataset"): RuntimeError("read failed"),
            ("src-proj", "other_dataset"): {self.SECOND_KEY: [column(column_name="b")]},
        }

        with pytest.raises(RuntimeError, match="src-proj.src_dataset"):
            harness.run([TARGET, self.SECOND_TARGET])

        # The second target still got its model calls.
        assert [r["column_name"] for r in harness.records] == ["b"]

    def test_unavailable_model_stops_the_run(self, monkeypatch):
        monkeypatch.setattr(runner, "MAX_CONSECUTIVE_FAILURES", 1)
        harness = Harness(answers=[RuntimeError("boom")]).install(monkeypatch)
        harness.columns_by_target = {
            ("src-proj", "src_dataset"): one_table([column(column_name="a")]),
            ("src-proj", "other_dataset"): {self.SECOND_KEY: [column(column_name="b")]},
        }

        with pytest.raises(ModelUnavailableError):
            harness.run([TARGET, self.SECOND_TARGET])

        assert harness.load_columns_calls == [("src-proj", "src_dataset", None)]
        assert harness.records == []
