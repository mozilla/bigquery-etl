import json
import os
from distutils.dir_util import copy_tree
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from unittest.mock import patch

import bigeye_sdk
import pytest
from bigeye_sdk.generated.com.bigeye.models.generated import MetricRunStatus
from click.testing import CliRunner

from bigquery_etl.cli.monitoring import (
    delete,
    deploy,
    deploy_custom_rules,
    run,
    set_partition_column,
    update,
    validate,
)

TEST_DIR = Path(__file__).parent.parent


class TestMonitoring:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    @pytest.fixture(autouse=True)
    def set_api_key(self):
        os.environ["BIGEYE_API_KEY"] = "test-api-key"

    @patch("bigeye_sdk.client.datawatch_client.datawatch_client_factory")
    @patch(
        "bigeye_sdk.controller.metric_suite_controller.MetricSuiteController.__init__"
    )
    def test_deploy(self, mock_metric_controller_init, mock_client_factory, runner):
        with runner.isolated_filesystem():
            test_query = (
                TEST_DIR
                / "data"
                / "test_sql"
                / "moz-fx-data-test-project"
                / "test"
                / "incremental_query_v1"
            )

            SQL_DIR = Path("sql/moz-fx-data-shared-prod/test/incremental_query_v1")
            os.makedirs(str(SQL_DIR))
            copy_tree(str(test_query), str(SQL_DIR))

            mock_metric_controller_init.return_value = None
            mock_client_factory.return_value = None
            with patch.object(
                bigeye_sdk.controller.metric_suite_controller.MetricSuiteController,
                "execute_bigconfig",
            ) as mock_execute_bigconfig:
                mock_execute_bigconfig.return_value = None
                runner.invoke(deploy, [f"{str(SQL_DIR)}"])

                mock_execute_bigconfig.assert_called_once()
                assert (SQL_DIR / "bigconfig.yml").exists()

    @patch("bigquery_etl.cli.monitoring.datawatch_client_factory")
    @patch(
        "bigeye_sdk.controller.metric_suite_controller.MetricSuiteController.__init__"
    )
    def test_deploy_custom_rule(
        self, mock_metric_controller_init, mock_client_factory, runner
    ):
        with runner.isolated_filesystem():
            test_query = (
                TEST_DIR
                / "data"
                / "test_sql"
                / "moz-fx-data-test-project"
                / "test"
                / "incremental_query_v1"
            )

            SQL_DIR = Path("sql/moz-fx-data-shared-prod/test/incremental_query_v1")
            os.makedirs(str(SQL_DIR))
            copy_tree(str(test_query), str(SQL_DIR))

            (SQL_DIR / "bigeye_custom_rules.sql").write_text(
                """
                -- {
                --   "name": "Custom check",
                --   "alert_conditions": "value",
                --   "range": {
                --     "min": 0,
                --     "max": 1
                --   },
                --   "collections": ["Test"],
                --   "owner": "",
                --   "schedule": "Default Schedule - 13:00 UTC"
                -- }
                SELECT
                COUNT(*)
                FROM
                `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`;
            """
            )

            mock_metric_controller_init.return_value = None
            mock_client = mock.Mock()
            mock_client_factory.return_value = mock_client
            mock_client.get_collections.return_value = mock.Mock(collections=[])
            mock_client.get_rules_for_source.return_value = mock.Mock(custom_rules=[])
            mock_client.get_named_schedule.return_value = mock.Mock(named_schedules=[])
            mock_client._call_datawatch.return_value = None

            runner.invoke(deploy_custom_rules, [f"{str(SQL_DIR)}"])

            # mock_client_factory._call_datawatch.assert_called_once()
            mock_client.get_collections.assert_called_once()
            mock_client.get_rules_for_source.assert_called_once()
            mock_client._call_datawatch.assert_called_once()
            expected_body = {
                "customRule": {
                    "name": "Custom check",
                    "sql": '/* { */ /*   "name": "Custom check", */ /*   "alert_conditions": "value", */ /*   '
                    + '"range": { */ /*     "min": 0, */ /*     "max": 1 */ /*   }, */ /*   "collections": ["Test"], */ /*   '
                    + '"owner": "", */ /*   "schedule": "Default Schedule - 13:00 UTC" */ /* } '
                    + "*/ SELECT COUNT(*) FROM `moz-fx-data-shared-prod.test.incremental_query_v1`",
                    "warehouseId": 1939,
                    "thresholdType": "CUSTOM_RULES_THRESHOLD_TYPE_COUNT",
                    "lowerThreshold": 0,
                    "upperThreshold": 1,
                    "owner": {"email": ""},
                    "collectionIds": [],
                }
            }
            assert mock_client._call_datawatch.call_args.kwargs["body"] == json.dumps(
                expected_body
            )

    def test_update(self, runner):
        with runner.isolated_filesystem():
            test_query = (
                TEST_DIR
                / "data"
                / "test_sql"
                / "moz-fx-data-test-project"
                / "test"
                / "incremental_query_v1"
            )

            SQL_DIR = Path("sql/moz-fx-data-shared-prod/test/incremental_query_v1")
            os.makedirs(str(SQL_DIR))
            copy_tree(str(test_query), str(SQL_DIR))

            assert not (SQL_DIR / "bigconfig.yml").exists()
            runner.invoke(update, [f"{str(SQL_DIR)}"])
            assert (SQL_DIR / "bigconfig.yml").exists()

    def test_update_existing_bigconfig(self, runner):
        with runner.isolated_filesystem():
            test_query = (
                TEST_DIR
                / "data"
                / "test_sql"
                / "moz-fx-data-test-project"
                / "test"
                / "incremental_query_v1"
            )

            SQL_DIR = Path("sql/moz-fx-data-shared-prod/test/incremental_query_v1")
            os.makedirs(str(SQL_DIR))
            copy_tree(str(test_query), str(SQL_DIR))
            (SQL_DIR / "bigconfig.yml").write_text(
                """
                type: BIGCONFIG_FILE
                table_deployments:
                - deployments:
                - fq_table_name: moz-fx-data-shared-prod.moz-fx-data-shared-prod.test.incremental_query_v1
                    table_metrics:
                    - metric_type:
                        type: PREDEFINED
                        predefined_metric: FRESHNESS
                    metric_schedule:
                        named_schedule:
                        name: Default Schedule - 13:00 UTC
            """
            )

            assert (SQL_DIR / "bigconfig.yml").exists()
            runner.invoke(update, [f"{str(SQL_DIR)}"])
            assert (SQL_DIR / "bigconfig.yml").exists()
            print((SQL_DIR / "bigconfig.yml").read_text())
            assert (
                "predefined_metric: FRESHNESS"
                in (SQL_DIR / "bigconfig.yml").read_text()
            )
            assert (
                "predefined_metric: VOLUME"
                not in (SQL_DIR / "bigconfig.yml").read_text()
            )

    def test_validate_no_bigconfig_file(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = Path("sql/moz-fx-data-shared-prod/test/test_no_file")
            os.makedirs(str(SQL_DIR))

            assert not (SQL_DIR / "bigconfig.yml").exists()
            result = runner.invoke(validate, [f"{str(SQL_DIR)}"])
            assert result.exit_code == 0

    def test_validate_empty_file(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = Path("sql/moz-fx-data-shared-prod/test/test_empty")
            os.makedirs(str(SQL_DIR))
            (SQL_DIR / "bigconfig.yml").write_text("")

            result = runner.invoke(validate, [f"{str(SQL_DIR)}"])
            assert result.exit_code == 1

    def test_validate_invalid_file(self, runner):
        with runner.isolated_filesystem():
            SQL_DIR = Path("sql/moz-fx-data-shared-prod/test/invalid")
            os.makedirs(str(SQL_DIR))
            (SQL_DIR / "bigconfig.yml").write_text("invalid")

            result = runner.invoke(validate, [f"{str(SQL_DIR)}"])
            assert result.exit_code == 1

    @patch("bigquery_etl.cli.monitoring.datawatch_client_factory")
    @patch(
        "bigeye_sdk.controller.metric_suite_controller.MetricSuiteController.__init__"
    )
    def test_set_partition_column(
        self, mock_metric_controller_init, mock_client_factory, runner
    ):
        with runner.isolated_filesystem():
            test_query = (
                TEST_DIR
                / "data"
                / "test_sql"
                / "moz-fx-data-test-project"
                / "test"
                / "view_with_metadata"
            )
            SQL_DIR = Path("sql/moz-fx-data-shared-prod/test/view_with_metadata")
            os.makedirs(str(SQL_DIR))
            copy_tree(str(test_query), str(SQL_DIR))

            mock_metric_controller_init.return_value = None
            mock_client = mock.Mock()
            mock_client_factory.return_value = mock_client
            mock_client.get_table_ids.return_value = [1234]
            mock_client.get_columns.return_value = mock.Mock(
                columns=[SimpleNamespace(column=SimpleNamespace(name="date", id=111))]
            )
            call_datawatch_mock = mock.Mock()
            mock_client._call_datawatch.return_value = call_datawatch_mock
            call_datawatch_mock.get.return_value = {
                "datasetId": 1234,
                "requiredPartitionColumnId": 111,
            }

            runner.invoke(
                set_partition_column, [f"{str(SQL_DIR)}"], catch_exceptions=False
            )

            mock_client._call_datawatch.assert_called_once()
            expected_body = {"columnId": 111}
            assert mock_client._call_datawatch.call_args.kwargs["body"] == json.dumps(
                expected_body
            )

    @patch("bigquery_etl.cli.monitoring.datawatch_client_factory")
    @patch(
        "bigeye_sdk.controller.metric_suite_controller.MetricSuiteController.__init__"
    )
    def test_delete(self, mock_metric_controller_init, mock_client_factory, runner):
        with runner.isolated_filesystem():
            test_query = (
                TEST_DIR
                / "data"
                / "test_sql"
                / "moz-fx-data-test-project"
                / "test"
                / "incremental_query_v1"
            )

            SQL_DIR = Path("sql/moz-fx-data-shared-prod/test/incremental_query_v1")
            os.makedirs(str(SQL_DIR))
            copy_tree(str(test_query), str(SQL_DIR))

            mock_metric_controller_init.return_value = None
            mock_client = mock.Mock()
            mock_client_factory.return_value = mock_client
            mock_client.get_rules_for_source.return_value = mock.Mock(custom_rules=[])
            mock_client.get_metric_info_batch_post.return_value = mock.Mock(
                metrics=[1234]
            )
            mock_client.delete_metrics.return_value = None

            runner.invoke(
                delete, [f"{str(SQL_DIR)}", "--metrics"], catch_exceptions=False
            )
            mock_client.delete_metrics.assert_called_once_with(metrics=[1234])

    @patch("bigquery_etl.cli.monitoring.datawatch_client_factory")
    @patch(
        "bigeye_sdk.controller.metric_suite_controller.MetricSuiteController.__init__"
    )
    def test_delete_custom_sql(
        self, mock_metric_controller_init, mock_client_factory, runner
    ):
        with runner.isolated_filesystem():
            test_query = (
                TEST_DIR
                / "data"
                / "test_sql"
                / "moz-fx-data-test-project"
                / "test"
                / "incremental_query_v1"
            )

            SQL_DIR = Path("sql/moz-fx-data-shared-prod/test/incremental_query_v1")
            os.makedirs(str(SQL_DIR))
            copy_tree(str(test_query), str(SQL_DIR))
            (SQL_DIR / "bigeye_custom_rules.sql").write_text("SELECT 2")

            mock_metric_controller_init.return_value = None
            mock_client = mock.Mock()
            mock_client_factory.return_value = mock_client
            rules_mock = mock.Mock(
                custom_rules=[
                    mock.Mock(custom_rule=SimpleNamespace(id=1, sql="SELECT 2"))
                ]
            )
            mock_client.get_rules_for_source.return_value = rules_mock
            mock_client.delete_custom_rule.return_value = None
            mock_client.delete_metrics.return_value = None

            runner.invoke(
                delete,
                [f"{str(SQL_DIR)}", "--custom-sql"],
                catch_exceptions=False,
            )

            mock_client.delete_metrics.assert_not_called()
            mock_client.delete_custom_rule.assert_called_once_with(
                rules_mock.custom_rules[0].id
            )

    @patch("bigquery_etl.cli.monitoring.datawatch_client_factory")
    @patch(
        "bigeye_sdk.controller.metric_suite_controller.MetricSuiteController.__init__"
    )
    def test_run(self, mock_metric_controller_init, mock_client_factory, runner):
        with runner.isolated_filesystem():
            test_query = (
                TEST_DIR
                / "data"
                / "test_sql"
                / "moz-fx-data-test-project"
                / "test"
                / "incremental_query_v1"
            )

            SQL_DIR = Path("sql/moz-fx-data-shared-prod/test/incremental_query_v1")
            os.makedirs(str(SQL_DIR))
            copy_tree(str(test_query), str(SQL_DIR))

            mock_metric_controller_init.return_value = None
            mock_client = mock.Mock()
            mock_client_factory.return_value = mock_client
            mock_client.run_metric_batch_async.return_value = []
            mock_client.get_rules_for_source.return_value = mock.Mock(custom_rules=[])
            mock_metric_info = mock.Mock(
                metrics=[
                    SimpleNamespace(
                        name="test", metric_configuration=SimpleNamespace(id=1234)
                    )
                ]
            )
            mock_client.get_metric_info_batch_post.return_value = mock_metric_info

            runner.invoke(
                run,
                [f"{str(SQL_DIR)}"],
                catch_exceptions=False,
            )

        mock_client.run_metric_batch_async.assert_called_once_with(
            metric_ids=[mock_metric_info.metrics[0].metric_configuration.id]
        )

    @patch("bigquery_etl.cli.monitoring.datawatch_client_factory")
    @patch(
        "bigeye_sdk.controller.metric_suite_controller.MetricSuiteController.__init__"
    )
    def test_run_fail(self, mock_metric_controller_init, mock_client_factory, runner):
        with runner.isolated_filesystem():
            test_query = (
                TEST_DIR
                / "data"
                / "test_sql"
                / "moz-fx-data-test-project"
                / "test"
                / "incremental_query_v1"
            )

            SQL_DIR = Path("sql/moz-fx-data-shared-prod/test/incremental_query_v1")
            os.makedirs(str(SQL_DIR))
            copy_tree(str(test_query), str(SQL_DIR))

            mock_metric_controller_init.return_value = None
            mock_client = mock.Mock()
            mock_client_factory.return_value = mock_client
            mock_client.run_metric_batch_async.return_value = [
                SimpleNamespace(
                    latest_metric_runs=[
                        SimpleNamespace(
                            status=MetricRunStatus.METRIC_RUN_STATUS_UPPERBOUND_CRITICAL
                        )
                    ],
                    metric_configuration=SimpleNamespace(id=123, name="test [fail]"),
                    active_issue=SimpleNamespace(display_name="error"),
                )
            ]
            mock_client.get_rules_for_source.return_value = mock.Mock(custom_rules=[])
            mock_metric_info = mock.Mock(
                metrics=[
                    SimpleNamespace(
                        name="test", metric_configuration=SimpleNamespace(id=1234)
                    )
                ]
            )
            mock_client.get_metric_info_batch_post.return_value = mock_metric_info

            result = runner.invoke(
                run,
                [f"{str(SQL_DIR)}"],
                catch_exceptions=False,
            )
            assert result.exit_code == 1

        mock_client.run_metric_batch_async.assert_called_once_with(
            metric_ids=[mock_metric_info.metrics[0].metric_configuration.id]
        )

    @patch("bigquery_etl.cli.monitoring.datawatch_client_factory")
    @patch(
        "bigeye_sdk.controller.metric_suite_controller.MetricSuiteController.__init__"
    )
    def test_run_warn(self, mock_metric_controller_init, mock_client_factory, runner):
        with runner.isolated_filesystem():
            test_query = (
                TEST_DIR
                / "data"
                / "test_sql"
                / "moz-fx-data-test-project"
                / "test"
                / "incremental_query_v1"
            )

            SQL_DIR = Path("sql/moz-fx-data-shared-prod/test/incremental_query_v1")
            os.makedirs(str(SQL_DIR))
            copy_tree(str(test_query), str(SQL_DIR))

            mock_metric_controller_init.return_value = None
            mock_client = mock.Mock()
            mock_client_factory.return_value = mock_client
            mock_client.run_metric_batch_async.return_value = [
                SimpleNamespace(
                    latest_metric_runs=[
                        SimpleNamespace(
                            status=MetricRunStatus.METRIC_RUN_STATUS_UPPERBOUND_CRITICAL
                        )
                    ],
                    metric_configuration=SimpleNamespace(id=123, name="test [warn]"),
                    active_issue=SimpleNamespace(display_name="error"),
                )
            ]
            mock_client.get_rules_for_source.return_value = mock.Mock(custom_rules=[])
            mock_metric_info = mock.Mock(
                metrics=[
                    SimpleNamespace(
                        name="test [warn]",
                        metric_configuration=SimpleNamespace(id=1234),
                    )
                ]
            )
            mock_client.get_metric_info_batch_post.return_value = mock_metric_info

            result = runner.invoke(
                run,
                [f"{str(SQL_DIR)}"],
                catch_exceptions=False,
            )
            assert result.exit_code == 0

        mock_client.run_metric_batch_async.assert_called_once_with(
            metric_ids=[mock_metric_info.metrics[0].metric_configuration.id]
        )

    @patch("bigquery_etl.cli.monitoring.datawatch_client_factory")
    @patch(
        "bigeye_sdk.controller.metric_suite_controller.MetricSuiteController.__init__"
    )
    def test_run_custom_rule(
        self, mock_metric_controller_init, mock_client_factory, runner
    ):
        with runner.isolated_filesystem():
            test_query = (
                TEST_DIR
                / "data"
                / "test_sql"
                / "moz-fx-data-test-project"
                / "test"
                / "incremental_query_v1"
            )

            SQL_DIR = Path("sql/moz-fx-data-shared-prod/test/incremental_query_v1")
            os.makedirs(str(SQL_DIR))
            copy_tree(str(test_query), str(SQL_DIR))
            (SQL_DIR / "bigeye_custom_rules.sql").write_text(
                """
                SELECT 1
            """
            )

            mock_metric_controller_init.return_value = None
            mock_client = mock.Mock()
            mock_client_factory.return_value = mock_client
            mock_client.run_metric_batch_async.return_value = []
            rules_mock = mock.Mock(
                custom_rules=[
                    mock.Mock(
                        custom_rule=SimpleNamespace(id=1, sql="SELECT 1", name="rule")
                    )
                ]
            )
            mock_client.get_rules_for_source.return_value = rules_mock
            mock_metric_info = mock.Mock(metrics=[])
            mock_client.get_metric_info_batch_post.return_value = mock_metric_info
            mock_datawatch = mock.Mock()
            mock_client._call_datawatch.return_value = mock_datawatch
            mock_datawatch.get.return_value = []

            runner.invoke(
                run,
                [f"{str(SQL_DIR)}"],
                catch_exceptions=False,
            )

            mock_client._call_datawatch.assert_called_once()
            assert (
                str(rules_mock.custom_rules[0].id)
                in mock_client._call_datawatch.call_args.kwargs["url"]
            )


class MockBigeyeClient:
    def __init__(*args, **kwargs):
        pass

    def collections(self):
        return
