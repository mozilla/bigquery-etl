import unittest.mock
from datetime import date
from functools import partial
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from bigquery_etl.cli.query import (  # noqa: F401  # keeps circular import from exploding
    run,
)
from bigquery_etl.copy_deduplicate import (
    _copy_join_parts,
    _field_paths,
    _get_query_job_configs,
    _has_field_path,
    _select_geo,
    copy_deduplicate,
)

PROJECT_ID = "moz-fx-data-shared-prod"

GLEAN_MAPPING = {
    "org_mozilla_firefox": "firefox",
    "ads_backend": "ads_backend",
}

VALID_GEO_DEPRECATION_SCHEMA = [
    bigquery.SchemaField("document_id", "STRING"),
    bigquery.SchemaField(
        "client_info",
        "RECORD",
        fields=[
            bigquery.SchemaField("client_id", "STRING"),
        ],
    ),
    bigquery.SchemaField(
        "metadata",
        "RECORD",
        fields=[
            bigquery.SchemaField(
                "geo",
                "RECORD",
                fields=[
                    bigquery.SchemaField("city", "STRING"),
                    bigquery.SchemaField("subdivision1", "STRING"),
                    bigquery.SchemaField("subdivision2", "STRING"),
                ],
            )
        ],
    ),
]

GEO_CONFIG = {
    "geo_deprecation": {
        "skip_apps": ["ads_backend"],
        "skip_tables": ["newtab"],
    }
}

LIVE_TABLE = "moz-fx-data-shared-prod.firefox_desktop_live.metrics_v1"
BASE_SCHEMA = [bigquery.SchemaField("document_id", "STRING")]
STABLE_TABLE = "moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1$20260101"


def _slice_query_jobs(slices):
    """Build query jobs for sliced mode, each writing to its own temporary table."""
    return [
        Mock(
            total_bytes_processed=1,
            dry_run=False,
            slot_millis=1,
            destination=bigquery.TableReference.from_string(
                f"moz-fx-data-shared-prod.tmp.temp_{i}"
            ),
        )
        for i in range(slices)
    ]


class TestCopyDeduplicate:

    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture
    def valid_geo_deprecation_schema_table(self):
        """Table with include_client_id=true and all required geo fields present."""
        table = Mock()
        table.labels = {"include_client_id": "true"}
        table.schema = VALID_GEO_DEPRECATION_SCHEMA
        return table

    @pytest.fixture
    def mock_bq_env(self, monkeypatch):
        """
        Fully self-contained environment:
          - Patches get_glean_app_id_to_app_name_mapping and ConfigLoader.get
          - Optionally wires a provided ClientQueue mock when client_queue_cls is given
          - Returns (mock_client, captured_calls, run_dedup_side_effect)
        """

        mapping_mock = Mock()
        config_get_mock = Mock()

        monkeypatch.setattr(
            "bigquery_etl.copy_deduplicate.get_glean_app_id_to_app_name_mapping",
            mapping_mock,
        )
        monkeypatch.setattr(
            "bigquery_etl.copy_deduplicate.ConfigLoader.get",
            config_get_mock,
        )

        def _configure(
            *,
            mapping: dict | None = None,
            config: dict | None = None,
            table=None,
            client_queue_cls=None,
        ):
            def make_config_side_effect(config_dict: dict):
                def _side_effect(section=None, key=None, fallback=None):
                    section_dict = config_dict.get(section)
                    if section_dict:
                        return section_dict.get(key, fallback)
                    return fallback

                return _side_effect

            mapping_mock.return_value = mapping or {}
            config_get_mock.side_effect = make_config_side_effect(config or {})

            mock_client = Mock(spec=bigquery.Client)
            if table is not None:
                mock_client.get_table.return_value = table

            captured_calls = None
            run_dedup_side_effect = None

            if client_queue_cls is not None:
                client_queue = client_queue_cls.return_value
                client_ctx = client_queue.client.return_value
                client_ctx.__enter__.return_value = mock_client
                client_ctx.__exit__.return_value = False

                client_queue.with_client.side_effect = (
                    lambda func, *args, **kwargs: func(mock_client, *args, **kwargs)
                )

                captured_calls = []

                def _run_dedup_side_effect(
                    client, sql, live_table, stable_table, job_config, num_retries
                ):
                    captured_calls.append(
                        (client, sql, live_table, stable_table, job_config, num_retries)
                    )
                    fake_job = Mock()
                    return live_table, stable_table, fake_job

                run_dedup_side_effect = _run_dedup_side_effect

            return mock_client, captured_calls, run_dedup_side_effect

        return _configure

    @patch("bigquery_etl.copy_deduplicate._copy_join_parts")
    @patch("bigquery_etl.copy_deduplicate._run_deduplication_query")
    @patch("bigquery_etl.copy_deduplicate._list_live_tables")
    @patch("bigquery_etl.copy_deduplicate.ClientQueue")
    def test_copy_deduplicate_geo_deprecation_sql(
        self,
        mock_client_queue_cls,
        mock_list_live_tables,
        mock_run_dedup,
        mock_copy_join_parts,
        runner,
        mock_bq_env,
        valid_geo_deprecation_schema_table,
    ):
        mock_client, captured_calls, run_dedup_side_effect = mock_bq_env(
            mapping=GLEAN_MAPPING,
            config=GEO_CONFIG,
            table=valid_geo_deprecation_schema_table,
            client_queue_cls=mock_client_queue_cls,
        )

        mock_run_dedup.side_effect = run_dedup_side_effect
        mock_copy_join_parts.return_value = None

        mock_list_live_tables.return_value = [
            f"{PROJECT_ID}.org_mozilla_firefox_live.baseline_v1",
            f"{PROJECT_ID}.org_mozilla_firefox_live.newtab_v1",
            f"{PROJECT_ID}.ads_backend_live.events_v1",
            f"{PROJECT_ID}.telemetry_live.events_v1",
        ]

        submission_date = "2020-11-01"

        result = runner.invoke(
            copy_deduplicate,
            [
                f"--project_id={PROJECT_ID}",
                f"--date={submission_date}",
            ],
        )

        assert result.exit_code == 0
        # 4 tables listed in mock_list_live_tables.return_value
        assert len(captured_calls) == 4

        partition = submission_date.replace("-", "")

        # 1) org_mozilla_firefox_live.baseline_v1: should include geo REPLACE clause
        _, sql_arg, live_table_arg, stable_table_arg, _, _ = captured_calls[0]
        assert "REPLACE (" in sql_arg
        assert "CAST(NULL AS STRING) AS city" in sql_arg
        assert (
            f"{mock_list_live_tables.return_value[0].replace('_live', '_stable')}${partition}"
            == stable_table_arg
        )
        assert mock_list_live_tables.return_value[0] == live_table_arg

        # 2) newtab_v1: should be skipped by skip_tables
        _, sql_arg, live_table_arg, stable_table_arg, _, _ = captured_calls[1]
        assert "REPLACE (" not in sql_arg
        assert (
            f"{mock_list_live_tables.return_value[1].replace('_live', '_stable')}${partition}"
            == stable_table_arg
        )
        assert mock_list_live_tables.return_value[1] == live_table_arg

        # 3) ads_backend_live: should be skipped by skip_apps
        _, sql_arg, live_table_arg, stable_table_arg, _, _ = captured_calls[2]
        assert "REPLACE (" not in sql_arg
        assert (
            f"{mock_list_live_tables.return_value[2].replace('_live', '_stable')}${partition}"
            == stable_table_arg
        )
        assert mock_list_live_tables.return_value[2] == live_table_arg

        # 4) telemetry_live: not in GLEAN_MAPPING
        _, sql_arg, live_table_arg, stable_table_arg, _, _ = captured_calls[3]
        assert "REPLACE (" not in sql_arg
        assert (
            f"{mock_list_live_tables.return_value[3].replace('_live', '_stable')}${partition}"
            == stable_table_arg
        )
        assert mock_list_live_tables.return_value[3] == live_table_arg

    def test_has_field_path_top_level_and_nested(self):
        # top-level field
        assert _has_field_path(VALID_GEO_DEPRECATION_SCHEMA, ["document_id"])
        assert not _has_field_path(VALID_GEO_DEPRECATION_SCHEMA, ["missing_top_level"])

        # nested fields
        assert _has_field_path(
            VALID_GEO_DEPRECATION_SCHEMA, ["client_info", "client_id"]
        )
        assert _has_field_path(
            VALID_GEO_DEPRECATION_SCHEMA, ["metadata", "geo", "city"]
        )
        assert _has_field_path(
            VALID_GEO_DEPRECATION_SCHEMA, ["metadata", "geo", "subdivision1"]
        )
        assert _has_field_path(
            VALID_GEO_DEPRECATION_SCHEMA, ["metadata", "geo", "subdivision2"]
        )
        assert not _has_field_path(
            VALID_GEO_DEPRECATION_SCHEMA, ["metadata", "geo", "nope"]
        )

    def test_select_geo_with_required_fields_present(
        self,
        valid_geo_deprecation_schema_table,
        mock_bq_env,
    ):
        """If geo fields exist and conditions pass, we should get the NULLing SQL."""
        mock_client, _, _ = mock_bq_env(
            mapping=GLEAN_MAPPING,
            config=GEO_CONFIG,
            table=valid_geo_deprecation_schema_table,
        )

        live_table = "moz-fx-data-shared-prod.org_mozilla_firefox_live.baseline_v1"
        sql = _select_geo(live_table, mock_client)

        assert (
            "org_mozilla_firefox_live" not in GEO_CONFIG["geo_deprecation"]["skip_apps"]
        )
        assert "baseline" not in GEO_CONFIG["geo_deprecation"]["skip_tables"]
        assert "REPLACE (" in sql
        assert "metadata.geo" in sql
        assert "CAST(NULL AS STRING) AS city" in sql
        assert "CAST(NULL AS STRING) AS subdivision1" in sql
        assert "CAST(NULL AS STRING) AS subdivision2" in sql

    def test_select_geo_when_skipped_app(
        self,
        mock_bq_env,
    ):
        """If app_name is in geo_deprecation.skip_apps, we should get an empty string."""
        mock_client, _, _ = mock_bq_env(
            mapping=GLEAN_MAPPING,
            config=GEO_CONFIG,
        )

        live_table = "moz-fx-data-shared-prod.ads_backend_live.events_v1"
        sql = _select_geo(live_table, mock_client)

        assert "ads_backend" in GEO_CONFIG["geo_deprecation"]["skip_apps"]
        assert sql == ""

    def test_select_geo_when_skipped_table(
        self,
        mock_bq_env,
    ):
        """If table is in geo_deprecation.skip_tables, we should get an empty string."""
        mock_client, _, _ = mock_bq_env(
            mapping=GLEAN_MAPPING,
            config=GEO_CONFIG,
        )

        live_table = "moz-fx-data-shared-prod.firefox_ios_live.newtab_v1"
        sql = _select_geo(live_table, mock_client)

        assert "newtab" in GEO_CONFIG["geo_deprecation"]["skip_tables"]
        assert sql == ""

    def test_select_geo_when_not_glean_app(
        self,
        mock_bq_env,
    ):
        """If app_id is not in the glean mapping, we should get an empty string."""
        mock_client, _, _ = mock_bq_env(
            mapping=GLEAN_MAPPING,
            config=GEO_CONFIG,
        )

        live_table = "moz-fx-data-shared-prod.telemetry_live.baseline_v1"
        sql = _select_geo(live_table, mock_client)

        assert "telemetry" not in GLEAN_MAPPING
        assert sql == ""

    def test_select_geo_when_client_id_label_false(
        self,
        mock_bq_env,
    ):
        """If include_client_id label is false, we should get an empty string."""
        table = Mock()
        table.labels = {"include_client_id": "false"}
        table.schema = VALID_GEO_DEPRECATION_SCHEMA

        mock_client, _, _ = mock_bq_env(
            mapping=GLEAN_MAPPING,
            config=GEO_CONFIG,
            table=table,
        )

        live_table = "moz-fx-data-shared-prod.org_mozilla_firefox_live.baseline_v1"
        sql = _select_geo(live_table, mock_client)

        assert table.labels["include_client_id"] == "false"
        assert sql == ""

    def test_select_geo_when_client_id_label_missing(
        self,
        mock_bq_env,
    ):
        """If include_client_id label is missing, we should get an empty string."""
        table = Mock()
        table.labels = {"owner1": "wichan"}  # no include_client_id
        table.schema = VALID_GEO_DEPRECATION_SCHEMA

        mock_client, _, _ = mock_bq_env(
            mapping=GLEAN_MAPPING,
            config=GEO_CONFIG,
            table=table,
        )

        live_table = "moz-fx-data-shared-prod.org_mozilla_firefox_live.baseline_v1"
        sql = _select_geo(live_table, mock_client)

        assert "include_client_id" not in table.labels
        assert sql == ""

    def test_select_geo_when_client_id_field_missing(
        self,
        mock_bq_env,
    ):
        """If one of the required geo fields is missing, we should get an empty string."""
        invalid_geo_deprecation_schema = [
            bigquery.SchemaField("document_id", "STRING"),
            bigquery.SchemaField(
                "client_info",
                "RECORD",
                fields=[
                    bigquery.SchemaField("os", "STRING"),
                ],
            ),
            bigquery.SchemaField(
                "metadata",
                "RECORD",
                fields=[
                    bigquery.SchemaField(
                        "geo",
                        "RECORD",
                        fields=[
                            bigquery.SchemaField("city", "STRING"),
                            bigquery.SchemaField("subdivision1", "STRING"),
                            bigquery.SchemaField("subdivision2", "STRING"),
                        ],
                    )
                ],
            ),
        ]

        table = Mock()
        table.labels = {"include_client_id": "true"}
        table.schema = invalid_geo_deprecation_schema

        mock_client, _, _ = mock_bq_env(
            mapping=GLEAN_MAPPING,
            config=GEO_CONFIG,
            table=table,
        )

        live_table = "moz-fx-data-shared-prod.org_mozilla_firefox_live.baseline_v1"
        sql = _select_geo(live_table, mock_client)

        assert not _has_field_path(
            invalid_geo_deprecation_schema, ["client_info", "client_id"]
        )
        assert sql == ""

    def test_select_geo_when_geo_field_missing(
        self,
        mock_bq_env,
    ):
        """If one of the required geo fields is missing, we should get an empty string."""
        invalid_geo_deprecation_schema = [
            bigquery.SchemaField("document_id", "STRING"),
            bigquery.SchemaField(
                "client_info",
                "RECORD",
                fields=[
                    bigquery.SchemaField("client_id", "STRING"),
                ],
            ),
            bigquery.SchemaField(
                "metadata",
                "RECORD",
                fields=[
                    bigquery.SchemaField(
                        "geo",
                        "RECORD",
                        fields=[
                            bigquery.SchemaField("subdivision1", "STRING"),
                            bigquery.SchemaField("subdivision2", "STRING"),
                        ],
                    )
                ],
            ),
        ]

        table = Mock()
        table.labels = {"include_client_id": "true"}
        table.schema = invalid_geo_deprecation_schema

        mock_client, _, _ = mock_bq_env(
            mapping=GLEAN_MAPPING,
            config=GEO_CONFIG,
            table=table,
        )

        live_table = "moz-fx-data-shared-prod.org_mozilla_firefox_live.baseline_v1"
        sql = _select_geo(live_table, mock_client)

        assert not _has_field_path(
            invalid_geo_deprecation_schema, ["metadata", "geo", "city"]
        )
        assert sql == ""

    def test_get_query_job_configs(self):
        mock_client = Mock(spec=bigquery.Client)

        def get_table(table_name):
            base_schema = [
                bigquery.SchemaField(
                    "metrics",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField(
                            "string",
                            "RECORD",
                            fields=(
                                [bigquery.SchemaField("metric1", "STRING")]
                                + (
                                    [bigquery.SchemaField("metric2", "STRING")]
                                    if table_name.split("$")[0].endswith("_v1")
                                    else []
                                )
                            ),
                        )
                    ],
                ),
            ]
            mock_table = Mock(spec=bigquery.Table)
            mock_table.schema = base_schema
            return mock_table

        mock_client.get_table.side_effect = get_table

        get_query_job_configs_partial = partial(
            _get_query_job_configs,
            client=mock_client,
            live_table="moz-fx-data-shared-prod.firefox_desktop_live.metrics_v1",
            submission_date=date(2026, 1, 1),
            dry_run=False,
            slices=1,
            priority="INTERACTIVE",
            preceding_days=0,
            num_retries=0,
            temp_dataset="",
        )

        true_job_config = get_query_job_configs_partial(write_to_v2=True)

        # query string
        assert "metrics.string.metric1" in true_job_config[0][0]
        assert "metrics.string.metric2" not in true_job_config[0][0]
        # stable table
        assert (
            "moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v2$20260101"
            == str(true_job_config[0][2])
        )
        # QueryJobConfig
        assert (
            "moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v2$20260101"
            == str(true_job_config[0][3].destination)
        )

        false_job_config = get_query_job_configs_partial(write_to_v2=False)

        # query string
        assert "metrics.string.metric1" not in false_job_config[0][0]
        assert "metrics.string.metric2" not in false_job_config[0][0]
        # stable table
        assert (
            "moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1$20260101"
            == str(false_job_config[0][2])
        )
        # QueryJobConfig
        assert (
            "moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1$20260101"
            == str(false_job_config[0][3].destination)
        )

    def test_copy_join_parts_write_to_v2_true(self):
        mock_client = Mock(spec=bigquery.Client)
        _copy_join_parts(
            client=mock_client,
            live_table=LIVE_TABLE,
            stable_table="moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v2$20260101",
            query_jobs=[Mock(total_bytes_processed=1, dry_run=False, slot_millis=1)],
            write_to_v2=True,
        )

        mock_client.copy_table.assert_called_once_with(
            "moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v2$20260101",
            "moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1$20260101",
            job_config=unittest.mock.ANY,
        )

    def test_copy_join_parts_write_to_v2_false(self):
        mock_client = Mock(spec=bigquery.Client)
        _copy_join_parts(
            client=mock_client,
            live_table=LIVE_TABLE,
            stable_table=STABLE_TABLE,
            query_jobs=[Mock(total_bytes_processed=1, dry_run=False, slot_millis=1)],
            write_to_v2=False,
        )

        assert mock_client.copy_table.call_count == 0

    def test_copy_join_parts_sliced_copy_succeeds(self):
        """When the copy succeeds, temporary table schemas are left untouched."""
        mock_client = Mock(spec=bigquery.Client)
        mock_client.get_table.return_value = Mock(schema=BASE_SCHEMA)

        _copy_join_parts(
            client=mock_client,
            live_table=LIVE_TABLE,
            stable_table=STABLE_TABLE,
            query_jobs=_slice_query_jobs(2),
            write_to_v2=False,
        )

        # single copy attempt, and no schema reconciliation
        assert mock_client.copy_table.call_count == 1
        assert mock_client.update_table.call_count == 0
        # temporary tables are still cleaned up
        assert mock_client.delete_table.call_count == 2

    def test_copy_join_parts_conforms_on_copy_failure(self):
        """
        If the copy fails because the slices' schemas diverged, the temporary tables are
        set to the live table's schema and the copy is retried without rerunning the slices.
        """
        mock_client = Mock(spec=bigquery.Client)

        # the live table's schema is a superset of every slice's
        live_schema = [
            bigquery.SchemaField("document_id", "STRING"),
            bigquery.SchemaField("new_field", "STRING"),
        ]
        # temp table 0 ran before the schema change and lacks new_field
        temp_schemas = {
            "temp_0": [bigquery.SchemaField("document_id", "STRING")],
            "temp_1": live_schema,
        }

        def get_table(table_id):
            if table_id == LIVE_TABLE:
                return Mock(schema=live_schema)
            if table_id == STABLE_TABLE.split("$")[0]:
                # the stable table schema update has already landed
                return Mock(schema=live_schema)
            return Mock(schema=temp_schemas[str(table_id).split(".")[-1]])

        mock_client.get_table.side_effect = get_table

        # first copy fails with a schema mismatch, second succeeds
        first_copy = Mock()
        first_copy.result.side_effect = BadRequest("Incompatible table schemata")
        mock_client.copy_table.side_effect = [first_copy, Mock()]

        _copy_join_parts(
            client=mock_client,
            live_table=LIVE_TABLE,
            stable_table=STABLE_TABLE,
            query_jobs=_slice_query_jobs(2),
            write_to_v2=False,
        )

        # copy was retried after conforming
        assert mock_client.copy_table.call_count == 2
        # only the slice that was missing a field is updated, to the live table's schema
        assert mock_client.update_table.call_count == 1
        table_arg, fields_arg = mock_client.update_table.call_args.args
        assert table_arg.schema == live_schema
        assert fields_arg == ["schema"]
        # temporary tables are cleaned up after the successful retry
        assert mock_client.delete_table.call_count == 2

    def test_copy_join_parts_conforms_nested_field(self):
        """A slice missing a nested field is conformed to the live table's schema."""
        mock_client = Mock(spec=bigquery.Client)

        live_schema = [
            bigquery.SchemaField(
                "metrics",
                "RECORD",
                fields=[
                    bigquery.SchemaField("metric1", "STRING"),
                    bigquery.SchemaField("metric2", "STRING"),
                ],
            )
        ]
        stale_schema = [
            bigquery.SchemaField(
                "metrics",
                "RECORD",
                fields=[bigquery.SchemaField("metric1", "STRING")],
            )
        ]

        def get_table(table_id):
            if table_id == LIVE_TABLE:
                return Mock(schema=live_schema)
            if table_id == STABLE_TABLE.split("$")[0]:
                return Mock(schema=live_schema)
            return Mock(schema=stale_schema)

        mock_client.get_table.side_effect = get_table

        first_copy = Mock()
        first_copy.result.side_effect = BadRequest("Incompatible table schemata")
        mock_client.copy_table.side_effect = [first_copy, Mock()]

        _copy_join_parts(
            client=mock_client,
            live_table=LIVE_TABLE,
            stable_table=STABLE_TABLE,
            query_jobs=_slice_query_jobs(2),
            write_to_v2=False,
        )

        assert mock_client.copy_table.call_count == 2
        # both slices are stale, so both get the live schema with the nested field
        assert mock_client.update_table.call_count == 2
        for call in mock_client.update_table.call_args_list:
            table_arg, _ = call.args
            assert table_arg.schema == live_schema

    def test_copy_join_parts_raises_when_copy_would_widen_stable(self):
        """
        Stable table schemas are managed externally, so a copy that would add a field to the
        stable table fails instead, naming the field.
        """
        mock_client = Mock(spec=bigquery.Client)

        stable_schema = [bigquery.SchemaField("document_id", "STRING")]
        temp_schema = stable_schema + [bigquery.SchemaField("brand_new", "STRING")]

        def get_table(table_id):
            if table_id == STABLE_TABLE.split("$")[0]:
                return Mock(schema=stable_schema)
            return Mock(schema=temp_schema)

        mock_client.get_table.side_effect = get_table

        with pytest.raises(ValueError, match="brand_new"):
            _copy_join_parts(
                client=mock_client,
                live_table=LIVE_TABLE,
                stable_table=STABLE_TABLE,
                query_jobs=_slice_query_jobs(2),
                write_to_v2=False,
            )

        # the copy never runs and the temporary tables are kept for a rerun
        assert mock_client.copy_table.call_count == 0
        assert mock_client.delete_table.call_count == 0

    def test_copy_join_parts_raises_when_conforming_would_widen_stable(self):
        """
        Conforming to the live schema must not add a new field into the stable table: the
        retried copy is checked too, so the widening is caught rather than applied.
        """
        mock_client = Mock(spec=bigquery.Client)

        # stable has not caught up with the live table's new field. The temp tables start out
        # within the stable schema (so the first copy passes the guard) but differ from each
        # other in field order, which is what the copy rejects.
        stable_schema = [
            bigquery.SchemaField("document_id", "STRING"),
            bigquery.SchemaField("other", "STRING"),
        ]
        live_schema = stable_schema + [bigquery.SchemaField("brand_new", "STRING")]
        temp_schemas = {
            "temp_0": stable_schema,
            "temp_1": list(reversed(stable_schema)),
        }
        conformed = {"done": False}

        def get_table(table_id):
            if table_id == LIVE_TABLE:
                return Mock(schema=live_schema)
            if table_id == STABLE_TABLE.split("$")[0]:
                return Mock(schema=stable_schema)
            name = str(table_id).split(".")[-1]
            # conforming applies the live schema, which has a field stable lacks
            if conformed["done"]:
                return Mock(schema=live_schema)
            return Mock(schema=temp_schemas[name])

        mock_client.get_table.side_effect = get_table
        mock_client.update_table.side_effect = lambda *a, **k: conformed.update(
            done=True
        )

        first_copy = Mock()
        first_copy.result.side_effect = BadRequest("Incompatible table schemata")
        mock_client.copy_table.return_value = first_copy

        with pytest.raises(ValueError, match="brand_new"):
            _copy_join_parts(
                client=mock_client,
                live_table=LIVE_TABLE,
                stable_table=STABLE_TABLE,
                query_jobs=_slice_query_jobs(2),
                write_to_v2=False,
            )

        # first copy attempted and failed, conforming happened, retry blocked by the guard
        assert mock_client.copy_table.call_count == 1
        assert mock_client.update_table.call_count > 0
        assert mock_client.delete_table.call_count == 0

    def test_field_paths_includes_nested_paths(self):
        schema = [
            bigquery.SchemaField("document_id", "STRING"),
            bigquery.SchemaField(
                "metrics",
                "RECORD",
                fields=[
                    bigquery.SchemaField("metric1", "STRING"),
                    bigquery.SchemaField(
                        "nested",
                        "RECORD",
                        fields=[bigquery.SchemaField("deep", "STRING")],
                    ),
                ],
            ),
        ]

        assert _field_paths(schema) == {
            "document_id",
            "metrics",
            "metrics.metric1",
            "metrics.nested",
            "metrics.nested.deep",
        }

    def test_copy_join_parts_reraises_other_bad_request(self):
        """
        A copy failure that isn't a schema mismatch is re-raised without conforming
        schemas or retrying, so unrelated errors aren't masked by a misleading retry.
        """
        mock_client = Mock(spec=bigquery.Client)
        mock_client.get_table.return_value = Mock(schema=BASE_SCHEMA)
        failed_copy = Mock()
        failed_copy.result.side_effect = BadRequest("Quota exceeded")
        mock_client.copy_table.return_value = failed_copy

        with pytest.raises(BadRequest):
            _copy_join_parts(
                client=mock_client,
                live_table=LIVE_TABLE,
                stable_table=STABLE_TABLE,
                query_jobs=_slice_query_jobs(2),
                write_to_v2=False,
            )

        # no conforming, retry, or deletion of temporary tables
        assert mock_client.copy_table.call_count == 1
        assert mock_client.update_table.call_count == 0
        assert mock_client.delete_table.call_count == 0
