from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner
from google.cloud import bigquery

from bigquery_etl.cli.query import (  # noqa: F401  # keeps circular import from exploding
    run,
)
from bigquery_etl.copy_deduplicate import _has_field_path, _select_geo, copy_deduplicate

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
    def configure_mock_bq_client(self, valid_geo_deprecation_schema_table):
        """
        Returns a function that configures ClientQueue + BigQuery client
        to always return the valid geo_deprecation table.
        """

        def _configure(mock_client_queue_cls):
            mock_client = Mock(spec=bigquery.Client)
            mock_client.get_table.return_value = valid_geo_deprecation_schema_table

            client_queue = mock_client_queue_cls.return_value
            client_ctx = client_queue.client.return_value
            client_ctx.__enter__.return_value = mock_client
            client_ctx.__exit__.return_value = False

            client_queue.with_client.side_effect = lambda func, *args, **kwargs: func(
                mock_client, *args, **kwargs
            )

            return mock_client

        return _configure

    @pytest.fixture
    def capture_run_dedup_calls(self):
        """
        Returns (captured_calls, side_effect_func) so we can plug the
        side effect into _run_deduplication_query and inspect later.
        """
        captured = []

        def _side_effect(client, sql, stable_table, job_config, num_retries):
            captured.append((client, sql, stable_table, job_config, num_retries))
            fake_query_job = Mock()
            return stable_table, fake_query_job

        return captured, _side_effect

    @pytest.fixture
    def geo_deprecation_config_side_effect(self):
        """ConfigLoader.get side effect for geo_deprecation."""

        def _side_effect(section=None, key=None, fallback=None):
            if section == "geo_deprecation":
                if key == "skip_apps":
                    return ["ads_backend"]
                if key == "skip_tables":
                    return ["newtab"]
            return fallback

        return _side_effect

    @patch("bigquery_etl.copy_deduplicate._copy_join_parts")
    @patch("bigquery_etl.copy_deduplicate._run_deduplication_query")
    @patch("bigquery_etl.copy_deduplicate._list_live_tables")
    @patch("bigquery_etl.copy_deduplicate.ConfigLoader.get")
    @patch("bigquery_etl.copy_deduplicate.get_glean_app_id_to_app_name_mapping")
    @patch("bigquery_etl.copy_deduplicate.ClientQueue")
    def test_copy_deduplicate_geo_deprecation_sql(
        self,
        mock_client_queue_cls,
        mock_mapping,
        mock_config_get,
        mock_list_live_tables,
        mock_run_dedup,
        mock_copy_join_parts,
        runner,
        geo_deprecation_config_side_effect,
        configure_mock_bq_client,
        capture_run_dedup_calls,
    ):
        mock_mapping.return_value = GLEAN_MAPPING
        mock_config_get.side_effect = geo_deprecation_config_side_effect
        configure_mock_bq_client(mock_client_queue_cls)
        captured_calls, run_dedup_side_effect = capture_run_dedup_calls
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
        _, sql_arg, stable_table_arg, _, _ = captured_calls[0]
        assert "REPLACE (" in sql_arg
        assert "CAST(NULL AS STRING) AS city" in sql_arg
        assert (
            f"{mock_list_live_tables.return_value[0].replace('_live', '_stable')}${partition}"
            == stable_table_arg
        )

        # 2) newtab_v1: should be skipped by skip_tables
        _, sql_arg, stable_table_arg, _, _ = captured_calls[1]
        assert "REPLACE (" not in sql_arg
        assert (
            f"{mock_list_live_tables.return_value[1].replace('_live', '_stable')}${partition}"
            == stable_table_arg
        )

        # 3) ads_backend_live: should be skipped by skip_apps
        _, sql_arg, stable_table_arg, _, _ = captured_calls[2]
        assert "REPLACE (" not in sql_arg
        assert (
            f"{mock_list_live_tables.return_value[2].replace('_live', '_stable')}${partition}"
            == stable_table_arg
        )

        # 4) telemetry_live: not in GLEAN_MAPPING
        _, sql_arg, stable_table_arg, _, _ = captured_calls[3]
        assert "REPLACE (" not in sql_arg
        assert (
            f"{mock_list_live_tables.return_value[3].replace('_live', '_stable')}${partition}"
            == stable_table_arg
        )

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

    @patch("bigquery_etl.copy_deduplicate.ConfigLoader.get")
    @patch("bigquery_etl.copy_deduplicate.get_glean_app_id_to_app_name_mapping")
    def test_select_geo_with_required_fields_present(
        self,
        mock_mapping,
        mock_config_get,
        valid_geo_deprecation_schema_table,
        geo_deprecation_config_side_effect,
    ):
        """If geo fields exist and conditions pass, we should get the NULLing SQL."""
        mock_mapping.return_value = GLEAN_MAPPING
        mock_config_get.side_effect = geo_deprecation_config_side_effect

        mock_client = Mock(spec=bigquery.Client)
        mock_client.get_table.return_value = valid_geo_deprecation_schema_table

        live_table = "moz-fx-data-shared-prod.org_mozilla_firefox_live.baseline_v1"
        sql = _select_geo(live_table, mock_client)

        assert "org_mozilla_firefox_live" not in geo_deprecation_config_side_effect(
            "geo_deprecation", "skip_apps"
        )
        assert "baseline" not in geo_deprecation_config_side_effect(
            "geo_deprecation", "skip_tables"
        )
        assert "REPLACE (" in sql
        assert "metadata.geo" in sql
        assert "CAST(NULL AS STRING) AS city" in sql
        assert "CAST(NULL AS STRING) AS subdivision1" in sql
        assert "CAST(NULL AS STRING) AS subdivision2" in sql

    @patch("bigquery_etl.copy_deduplicate.ConfigLoader.get")
    @patch("bigquery_etl.copy_deduplicate.get_glean_app_id_to_app_name_mapping")
    def test_select_geo_when_skipped_app(
        self,
        mock_mapping,
        mock_config_get,
        geo_deprecation_config_side_effect,
    ):
        """If app_name is in geo_deprecation.skip_apps, we should get an empty string."""
        mock_mapping.return_value = GLEAN_MAPPING
        mock_config_get.side_effect = geo_deprecation_config_side_effect
        mock_client = Mock(spec=bigquery.Client)

        live_table = "moz-fx-data-shared-prod.ads_backend_live.events_v1"
        sql = _select_geo(live_table, mock_client)

        assert "ads_backend" in geo_deprecation_config_side_effect(
            "geo_deprecation", "skip_apps"
        )
        assert sql == ""

    @patch("bigquery_etl.copy_deduplicate.ConfigLoader.get")
    @patch("bigquery_etl.copy_deduplicate.get_glean_app_id_to_app_name_mapping")
    def test_select_geo_when_skipped_table(
        self,
        mock_mapping,
        mock_config_get,
        geo_deprecation_config_side_effect,
    ):
        """If table is in geo_deprecation.skip_tables, we should get an empty string."""
        mock_mapping.return_value = GLEAN_MAPPING
        mock_config_get.side_effect = geo_deprecation_config_side_effect
        mock_client = Mock(spec=bigquery.Client)

        live_table = "moz-fx-data-shared-prod.firefox_ios_live.newtab_v1"
        sql = _select_geo(live_table, mock_client)

        assert "newtab" in geo_deprecation_config_side_effect(
            "geo_deprecation", "skip_tables"
        )
        assert sql == ""

    @patch("bigquery_etl.copy_deduplicate.ConfigLoader.get")
    @patch("bigquery_etl.copy_deduplicate.get_glean_app_id_to_app_name_mapping")
    def test_select_geo_when_not_glean_app(
        self,
        mock_mapping,
        mock_config_get,
        geo_deprecation_config_side_effect,
    ):
        """If app_id is not in the glean mapping, we should get an empty string."""
        mock_mapping.return_value = GLEAN_MAPPING
        mock_config_get.side_effect = geo_deprecation_config_side_effect
        mock_client = Mock(spec=bigquery.Client)

        live_table = "moz-fx-data-shared-prod.telemetry_live.baseline_v1"
        sql = _select_geo(live_table, mock_client)

        assert "telemetry" not in GLEAN_MAPPING
        assert sql == ""

    @patch("bigquery_etl.copy_deduplicate.ConfigLoader.get")
    @patch("bigquery_etl.copy_deduplicate.get_glean_app_id_to_app_name_mapping")
    def test_select_geo_when_client_id_label_false(
        self,
        mock_mapping,
        mock_config_get,
        geo_deprecation_config_side_effect,
    ):
        """If include_client_id label is false, we should get an empty string."""
        mock_mapping.return_value = GLEAN_MAPPING
        mock_config_get.side_effect = geo_deprecation_config_side_effect

        table = Mock()
        table.labels = {"include_client_id": "false"}
        table.schema = VALID_GEO_DEPRECATION_SCHEMA

        mock_client = Mock(spec=bigquery.Client)
        mock_client.get_table.return_value = table

        live_table = "moz-fx-data-shared-prod.org_mozilla_firefox_live.baseline_v1"
        sql = _select_geo(live_table, mock_client)

        assert table.labels["include_client_id"] == "false"
        assert sql == ""

    @patch("bigquery_etl.copy_deduplicate.ConfigLoader.get")
    @patch("bigquery_etl.copy_deduplicate.get_glean_app_id_to_app_name_mapping")
    def test_select_geo_when_client_id_label_missing(
        self,
        mock_mapping,
        mock_config_get,
        geo_deprecation_config_side_effect,
    ):
        """If include_client_id label is missing, we should get an empty string."""
        mock_mapping.return_value = GLEAN_MAPPING
        mock_config_get.side_effect = geo_deprecation_config_side_effect

        table = Mock()
        table.labels = {"owner1": "wichan"}  # no include_client_id
        table.schema = VALID_GEO_DEPRECATION_SCHEMA

        mock_client = Mock(spec=bigquery.Client)
        mock_client.get_table.return_value = table

        live_table = "moz-fx-data-shared-prod.org_mozilla_firefox_live.baseline_v1"
        sql = _select_geo(live_table, mock_client)

        assert "include_client_id" not in table.labels
        assert sql == ""

    @patch("bigquery_etl.copy_deduplicate.ConfigLoader.get")
    @patch("bigquery_etl.copy_deduplicate.get_glean_app_id_to_app_name_mapping")
    def test_select_geo_when_geo_fields_missing(
        self,
        mock_mapping,
        mock_config_get,
        geo_deprecation_config_side_effect,
    ):
        """If one of the required geo fields is missing, we should get an empty string."""
        mock_mapping.return_value = GLEAN_MAPPING
        mock_config_get.side_effect = geo_deprecation_config_side_effect

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

        mock_client = Mock(spec=bigquery.Client)
        mock_client.get_table.return_value = table

        live_table = "moz-fx-data-shared-prod.org_mozilla_firefox_live.baseline_v1"
        sql = _select_geo(live_table, mock_client)

        assert not _has_field_path(
            invalid_geo_deprecation_schema, ["metadata", "geo", "city"]
        )
        assert sql == ""
