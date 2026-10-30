import re
from datetime import date

import pytest

from bigquery_etl.util.bq_query_args import (
    UnsupportedQueryArgumentsError,
    parse_bq_query_args,
)


class TestParseBqQueryArgs:
    def test_airflow_arguments(self):
        project, job_config = parse_bq_query_args(
            [
                "query",
                "--project_id=moz-fx-data-shared-prod",
                "--dataset_id=telemetry_derived",
                "--destination_table=clients_daily_v6$20240101",
                "--replace",
                "--use_legacy_sql=False",
                "--format=none",
                "--max_rows=0",
                "--parameter=submission_date:DATE:2024-01-01",
                "--parameter=name::abc",
                "--schema_update_option=ALLOW_FIELD_ADDITION",
            ]
        )

        assert project == "moz-fx-data-shared-prod"
        assert job_config.use_legacy_sql is False
        assert job_config.dry_run is False
        assert job_config.default_dataset.dataset_id == "telemetry_derived"
        assert job_config.destination.project == "moz-fx-data-shared-prod"
        assert job_config.destination.dataset_id == "telemetry_derived"
        assert job_config.destination.table_id == "clients_daily_v6$20240101"
        assert job_config.write_disposition == "WRITE_TRUNCATE"
        assert job_config.schema_update_options == ["ALLOW_FIELD_ADDITION"]
        params = {p.name: (p.type_, p.value) for p in job_config.query_parameters}
        assert params == {
            "submission_date": ("DATE", date(2024, 1, 1)),
            "name": ("STRING", "abc"),
        }

    def test_qualified_destination_and_session(self):
        project, job_config = parse_bq_query_args(
            [
                "query",
                "--project_id=billing-project",
                "--destination_table=other-project:dataset.table",
                "--append_table",
                "--session_id=abc",
                "--time_partitioning_field=submission_date",
                "--clustering_fields=a,b",
            ]
        )

        assert project == "billing-project"
        assert job_config.destination.project == "other-project"
        assert job_config.write_disposition == "WRITE_APPEND"
        assert job_config.connection_properties[0].value == "abc"
        assert job_config.time_partitioning.field == "submission_date"
        assert job_config.time_partitioning.type_ == "DAY"
        assert job_config.clustering_fields == ["a", "b"]

    def test_dry_run_without_destination(self):
        _, job_config = parse_bq_query_args(
            ["query", "--project_id=p", "--use_legacy_sql=false", "--dry_run"]
        )
        assert job_config.dry_run is True

    def test_space_separated_values(self):
        # e.g. the events_stream_v1 generator's scheduling arguments
        _, job_config = parse_bq_query_args(
            [
                "query",
                "--project_id",
                "p",
                "--destination_table=p.d.t",
                "--schema_update_option",
                "ALLOW_FIELD_ADDITION",
                "--parameter",
                "submission_date:DATE:2024-01-01",
            ]
        )
        assert job_config.schema_update_options == ["ALLOW_FIELD_ADDITION"]
        assert job_config.query_parameters[0].name == "submission_date"

    def test_append_table_noreplace(self):
        # used by `query initialize` and some scheduled queries
        _, job_config = parse_bq_query_args(
            [
                "query",
                "--project_id=p",
                "--destination_table=d.t",
                "--format=none",
                "--append_table",
                "--noreplace",
                "--parameter=sample_id:INT64:0",
            ]
        )
        assert job_config.write_disposition == "WRITE_APPEND"

    def test_noreplace(self):
        _, job_config = parse_bq_query_args(
            ["--project_id=p", "--destination_table=d.t", "--noreplace"]
        )
        assert job_config.write_disposition == "WRITE_EMPTY"

    def test_replace_takes_precedence_over_append_table(self):
        # like bq
        _, job_config = parse_bq_query_args(
            ["--project_id=p", "--destination_table=d.t", "--replace", "--append_table"]
        )
        assert job_config.write_disposition == "WRITE_TRUNCATE"

    def test_max_rows_zero_without_destination(self):
        _, job_config = parse_bq_query_args(["query", "--project_id=p", "--max_rows=0"])
        assert job_config.destination is None

    def test_null_parameter(self):
        _, job_config = parse_bq_query_args(
            ["query", "--project_id=p", "--format=none", "--parameter=a:INT64:NULL"]
        )
        parameter = job_config.query_parameters[0]
        assert parameter.value is None
        # like bq, which sends {"value": None}
        assert parameter.to_api_repr()["parameterValue"] == {"value": None}

    def test_json_parameter(self):
        _, job_config = parse_bq_query_args(
            [
                "query",
                "--project_id=p",
                "--format=none",
                '--parameter=a:JSON:{"b": [1, 2]}',
            ]
        )
        parameter = job_config.query_parameters[0]
        assert parameter.to_api_repr()["parameterValue"] == {"value": '{"b": [1, 2]}'}

    @pytest.mark.parametrize(
        "args,reason",
        [
            (["query", "--project_id=p"], "would be printed"),
            (["query", "--project_id=p", "--format=pretty"], "would be printed"),
            (["query", "--project_id=p", "--max_rows=10"], "would be printed"),
            (
                ["query", "--project_id=p", "--format=none", "--label=a:b"],
                "Unsupported flag `--label=a:b`",
            ),
            (
                ["query", "--project_id=p", "--format=none", "--parameter"],
                "Missing value for `--parameter`",
            ),
            (
                [
                    "query",
                    "--project_id=p",
                    "--format=none",
                    "--parameter",
                    "--dry_run",
                ],
                "Missing value for `--parameter`",
            ),
            (["query", "p.d.t"], "Unexpected argument `p.d.t`"),
            (
                ["query", "--project_id=p", "--format=none", "--use_legacy_sql=true"],
                "Legacy SQL",
            ),
            (
                ["query", "--project_id=p", "--format=none", "--dry_run=maybe"],
                "Unexpected boolean value",
            ),
            (
                ["query", "--project_id=p", "--format=none", "--parameter=:INT64:1"],
                "Positional parameter",
            ),
            (
                [
                    "query",
                    "--project_id=p",
                    "--format=none",
                    "--parameter=a:ARRAY<INT64>:[1]",
                ],
                "Parameter type `ARRAY<INT64>`",
            ),
            (
                ["query", "--project_id=p", "--format=none", "--parameter=a:1"],
                "name:type:value",
            ),
            (
                ["query", "--project_id=p", "--format=none", "--parameter=a:JSON:{"],
                "isn't valid JSON",
            ),
            (
                ["query", "--project_id=p", "--format=none", "--parameter=a:JSON:null"],
                "JSON null",
            ),
            # destination can't be resolved without a project
            (
                ["query", "--destination_table=table_v1", "--dataset_id=test"],
                "Can't resolve",
            ),
        ],
    )
    def test_unsupported(self, args, reason):
        with pytest.raises(UnsupportedQueryArgumentsError, match=re.escape(reason)):
            parse_bq_query_args(args)
