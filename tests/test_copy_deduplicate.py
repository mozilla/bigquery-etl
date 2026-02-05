import unittest.mock
from datetime import date
from functools import partial
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner
from google.cloud import bigquery

from bigquery_etl.cli.query import (  # noqa: F401  # keeps circular import from exploding
    run,
)
from bigquery_etl.copy_deduplicate import (
    _copy_join_parts,
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
            stable_table="moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1$20260101",
            query_jobs=[Mock(total_bytes_processed=1, dry_run=False, slot_millis=1)],
            write_to_v2=False,
        )

        assert mock_client.copy_table.call_count == 0
