from types import SimpleNamespace

import pytest
from google.cloud import bigquery

from bigquery_etl.data_governance.classification.config import ClassificationConfig
from bigquery_etl.data_governance.classification.inputs import (
    load_already_classified,
    load_columns,
    load_ping_mapping,
    load_probes_by_ping,
)

CONFIG = ClassificationConfig(run_id="run-1", project="proj", dataset="meta")


def row(**fields):
    """Build a real bigquery.Row, which supports both attribute and [] access."""
    names = list(fields)
    return bigquery.Row(
        tuple(fields[name] for name in names),
        {name: index for index, name in enumerate(names)},
    )


def column_row(**overrides):
    """A load_columns result row, with the joined profile present by default."""
    fields = {
        "table_name": "my_table",
        "field_path": "id",
        "data_type": "STRING",
        "description": "the id",
        "is_glean_metric": False,
        "column_tier": "scalar",
        "null_rate": 1.0,
        "distinct_count": 200,
        "is_high_cardinality": False,
        "example_value": "abc",
        "values": [{"value": "abc", "frequency": 3}],
    }
    fields.update(overrides)
    return row(**fields)


class FakeBigQueryClient:
    """Records the queries it is given and returns canned rows."""

    def __init__(self, rows=None):
        self.rows = rows or []
        self.queries = []
        self.job_configs = []

    def query(self, query, job_config=None):
        self.queries.append(query)
        self.job_configs.append(job_config)
        return SimpleNamespace(result=lambda: list(self.rows))


def parameters(client):
    """The single query's parameters as {name: value}."""
    return {p.name: p.value for p in client.job_configs[0].query_parameters}


def normalized(query):
    """Collapse the query's whitespace, so a clause match ignores its layout."""
    return " ".join(query.split())


class TestLoadColumns:
    def test_column_dict_holds_what_the_prompt_reads(self):
        client = FakeBigQueryClient([column_row()])

        result = load_columns(client, CONFIG, "src-proj", "src_dataset")

        assert result == {
            ("src-proj", "src_dataset", "my_table"): [
                {
                    "column_name": "id",
                    "data_type": "STRING",
                    "bq_description": "the id",
                    "is_glean_metric": False,
                    "column_tier": "scalar",
                    "null_rate": 1.0,
                    "distinct_count": 200,
                    "is_high_cardinality": False,
                    "example_value": "abc",
                    "values": [("abc", 3)],
                }
            ]
        }

    def test_unprofiled_column_keeps_the_schema_and_no_stats(self):
        # What the LEFT JOIN yields for a column the profiler has no row for.
        client = FakeBigQueryClient(
            [
                column_row(
                    field_path="metrics.labeled_counter.foo",
                    data_type="ARRAY<STRUCT<key STRING, value INT64>>",
                    description=None,
                    is_glean_metric=True,
                    column_tier=None,
                    null_rate=None,
                    distinct_count=None,
                    is_high_cardinality=None,
                    example_value=None,
                    values=None,
                )
            ]
        )

        columns = load_columns(client, CONFIG, "src-proj", "src_dataset")
        (column,) = columns[("src-proj", "src_dataset", "my_table")]

        assert column["column_name"] == "metrics.labeled_counter.foo"
        assert column["data_type"] == "ARRAY<STRUCT<key STRING, value INT64>>"
        assert column["bq_description"] is None
        assert column["is_glean_metric"] is True
        assert column["column_tier"] is None
        assert column["null_rate"] is None
        assert column["values"] == []

    def test_rows_group_by_table_key(self):
        client = FakeBigQueryClient(
            [
                column_row(table_name="table_a", field_path="id"),
                column_row(table_name="table_a", field_path="email"),
                column_row(table_name="table_b", field_path="id"),
            ]
        )

        result = load_columns(client, CONFIG, "src-proj", "src_dataset")

        assert sorted(result) == [
            ("src-proj", "src_dataset", "table_a"),
            ("src-proj", "src_dataset", "table_b"),
        ]
        assert [
            c["column_name"] for c in result[("src-proj", "src_dataset", "table_a")]
        ] == [
            "id",
            "email",
        ]

    def test_dataset_scope_is_parameterized_and_table_omitted(self):
        client = FakeBigQueryClient([])

        load_columns(client, CONFIG, "src-proj", "src_dataset")

        assert parameters(client) == {"project": "src-proj", "dataset": "src_dataset"}
        assert "@table" not in client.queries[0]

    def test_table_filter_is_a_parameter_not_interpolated(self):
        client = FakeBigQueryClient([])

        load_columns(client, CONFIG, "src-proj", "src_dataset", "my_table")

        assert parameters(client)["table"] == "my_table"
        assert "AND source_table = @table" in client.queries[0]
        assert "AND c.table_name = @table" in client.queries[0]
        assert "my_table" not in client.queries[0]

    def test_empty_table_name_narrows_rather_than_widens(self):
        # The predicate and the parameter that fills it are driven by the same
        # test, so an empty name asks for a table that does not exist instead of
        # quietly asking for the whole dataset.
        client = FakeBigQueryClient([])

        load_columns(client, CONFIG, "src-proj", "src_dataset", "")

        assert parameters(client)["table"] == ""
        assert "AND c.table_name = @table" in client.queries[0]

    def test_reads_the_dataset_information_schema_and_the_profiles_table(self):
        client = FakeBigQueryClient([])

        load_columns(client, CONFIG, "src-proj", "src_dataset")

        assert (
            "`src-proj.src_dataset`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS"
            in client.queries[0]
        )
        assert f"`{CONFIG.profiles_table}`" in client.queries[0]

    @pytest.mark.parametrize(
        "project,dataset",
        [
            ("src-proj", "src_dataset`) UNION ALL (SELECT"),
            ("src-proj", "src dataset"),
            ("src`proj", "src_dataset"),
        ],
        ids=["backtick_in_dataset", "space_in_dataset", "backtick_in_project"],
    )
    def test_bad_identifier_raises_before_querying(self, project, dataset):
        client = FakeBigQueryClient([])

        with pytest.raises(ValueError, match="Invalid BigQuery identifier"):
            load_columns(client, CONFIG, project, dataset)

        assert client.queries == []


class TestLoadColumnsQueryClauses:
    """Tripwires on the clauses a simplifying rewrite would quietly break."""

    @staticmethod
    def query():
        client = FakeBigQueryClient([])
        load_columns(client, CONFIG, "src-proj", "src_dataset")
        # Normalized, so reflowing or re-indenting the SQL does not fail a
        # tripwire that is about the clause and not about its layout.
        return normalized(client.queries[0])

    def test_latest_profile_is_picked_per_column_not_per_partition(self):
        query = self.query()

        assert "QUALIFY ROW_NUMBER() OVER (" in query
        assert (
            "PARTITION BY source_table, column_name ORDER BY profiled_at DESC" in query
        )
        assert "MAX(profiled_at)" not in query

    def test_scope_is_the_profiled_tables(self):
        assert "IN (SELECT DISTINCT source_table FROM profiles)" in self.query()

    def test_container_filter_is_confined_to_non_metric_columns(self):
        # Collapsing the OR back into a flat AND would silently drop every
        # structured Glean metric type: labeled metrics, distributions, timespans.
        assert (
            "OR (NOT c.is_glean_metric "
            "AND c.data_type NOT LIKE 'STRUCT<%' "
            "AND c.data_type NOT LIKE 'ARRAY<STRUCT<%')" in self.query()
        )

    def test_metric_columns_are_selected_by_depth_alone(self):
        assert (
            "(c.is_glean_metric AND ARRAY_LENGTH(SPLIT(c.field_path, '.')) = 3)"
            in self.query()
        )

    def test_glean_metric_is_decided_by_the_resolved_platform(self):
        # On the field path alone, a derived table's own `metrics` STRUCT (e.g.
        # telemetry_derived.smoot_usage_*) would be taken for Glean metrics, and
        # its columns dropped for want of an exactly matching probe.
        query = self.query()

        assert (
            "(STARTS_WITH(c.field_path, 'metrics.') "
            "AND COALESCE(l.ping_platform, 'glean') = 'glean') AS is_glean_metric"
            in query
        )
        assert f"`{CONFIG.lineage_table}`" in query
        assert "PARTITION BY source_table ORDER BY resolved_at DESC" in query


class TestLoadPingMapping:
    def test_keyed_by_the_table_triple(self):
        client = FakeBigQueryClient(
            [
                row(
                    source_project="src-proj",
                    source_dataset="fenix",
                    source_table="baseline_v1",
                    source_ping="fenix.baseline",
                    ping_platform="glean",
                ),
                row(
                    source_project="src-proj",
                    source_dataset="telemetry",
                    source_table="main_v5",
                    source_ping="main",
                    ping_platform="legacy_telemetry",
                ),
            ]
        )

        mapping = load_ping_mapping(client, CONFIG)

        assert mapping == {
            ("src-proj", "fenix", "baseline_v1"): {
                "source_ping": "fenix.baseline",
                "ping_platform": "glean",
            },
            ("src-proj", "telemetry", "main_v5"): {
                "source_ping": "main",
                "ping_platform": "legacy_telemetry",
            },
        }

    def test_query_reads_the_lineage_table_latest_resolution_first(self):
        client = FakeBigQueryClient([])

        load_ping_mapping(client, CONFIG)

        query = normalized(client.queries[0])
        assert "QUALIFY ROW_NUMBER() OVER (" in query
        assert "ORDER BY resolved_at DESC" in query
        assert f"`{CONFIG.lineage_table}`" in query


class TestLoadProbesByPing:
    def test_probe_dict_holds_what_the_prompt_reads(self):
        client = FakeBigQueryClient(
            [
                row(
                    ping_platform="glean",
                    source_ping="fenix.baseline",
                    probe_name="account.user_id",
                    probe_description="The account id.",
                    probe_type="string",
                    data_sensitivity=["highly_sensitive"],
                    tags=["Account"],
                ),
                row(
                    ping_platform="glean",
                    source_ping="fenix.baseline",
                    probe_name="ping.count",
                    probe_description=None,
                    probe_type="counter",
                    data_sensitivity=None,
                    tags=None,
                ),
            ]
        )

        probes = load_probes_by_ping(client, CONFIG)

        assert probes == {
            ("glean", "fenix.baseline"): [
                {
                    "probe_name": "account.user_id",
                    "probe_description": "The account id.",
                    "probe_type": "string",
                    "data_sensitivity": ["highly_sensitive"],
                    "tags": ["Account"],
                },
                {
                    "probe_name": "ping.count",
                    "probe_description": None,
                    "probe_type": "counter",
                    "data_sensitivity": [],
                    "tags": [],
                },
            ]
        }

    def test_grouped_by_platform_and_ping(self):
        client = FakeBigQueryClient(
            [
                row(
                    ping_platform="glean",
                    source_ping="fenix.baseline",
                    probe_name="a",
                    probe_description=None,
                    probe_type="counter",
                    data_sensitivity=[],
                    tags=[],
                ),
                row(
                    ping_platform="glean",
                    source_ping="firefox_desktop.baseline",
                    probe_name="a",
                    probe_description=None,
                    probe_type="counter",
                    data_sensitivity=[],
                    tags=[],
                ),
            ]
        )

        probes = load_probes_by_ping(client, CONFIG)

        assert sorted(probes) == [
            ("glean", "fenix.baseline"),
            ("glean", "firefox_desktop.baseline"),
        ]

    def test_query_reads_the_probes_table_latest_snapshot_first(self):
        client = FakeBigQueryClient([])

        load_probes_by_ping(client, CONFIG)

        query = normalized(client.queries[0])
        assert "QUALIFY ROW_NUMBER() OVER (" in query
        assert "ORDER BY fetched_at DESC" in query
        assert f"`{CONFIG.probes_table}`" in query


class TestLoadAlreadyClassified:
    def test_returns_four_tuples(self):
        client = FakeBigQueryClient(
            [
                row(
                    source_project="src-proj",
                    source_dataset="fenix",
                    source_table="baseline_v1",
                    column_name="client_info.client_id",
                ),
                row(
                    source_project="src-proj",
                    source_dataset="fenix",
                    source_table="baseline_v1",
                    column_name="id",
                ),
            ]
        )

        done = load_already_classified(client, CONFIG, "src-proj", "fenix")

        assert done == {
            ("src-proj", "fenix", "baseline_v1", "client_info.client_id"),
            ("src-proj", "fenix", "baseline_v1", "id"),
        }

    def test_scoped_to_the_dataset_and_the_run_model(self):
        client = FakeBigQueryClient([])

        load_already_classified(client, CONFIG, "src-proj", "fenix")

        assert parameters(client) == {
            "project": "src-proj",
            "dataset": "fenix",
            "model": CONFIG.model,
        }
        assert f"`{CONFIG.destination_table}`" in client.queries[0]
        assert "taxonomy_version" not in client.queries[0]

    def test_table_filter_is_a_parameter_not_interpolated(self):
        client = FakeBigQueryClient([])

        load_already_classified(client, CONFIG, "src-proj", "fenix", "baseline_v1")

        assert parameters(client)["table"] == "baseline_v1"
        assert "AND source_table = @table" in client.queries[0]
        assert "baseline_v1" not in client.queries[0]
