import os

import pytest
import yaml
from click.testing import CliRunner

from bigquery_etl.cli.routine import create, info, rename


class TestRoutine:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_create_invalid_path(self, runner):
        with runner.isolated_filesystem():
            with open("foo.txt", "w") as f:
                f.write("")
            result = runner.invoke(create, ["udf.test_udf", "--path=foo.txt"])
            assert result.exit_code == 2

    def test_create_invalid_name(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/udf")
            result = runner.invoke(
                create,
                ["udf.udf.test_udf", "--udf"],
                obj={"DEFAULT_PROJECT": "moz-fx-data-shared-prod"},
            )
            assert (
                "New routine must be named like: <dataset>.<routine_name>"
                in result.output
            )
            assert result.exit_code == 1

    def test_create_missing_udf_procedure(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/udf")
            result = runner.invoke(
                create,
                ["udf.test_udf"],
                obj={"DEFAULT_PROJECT": "moz-fx-data-shared-prod"},
            )
            assert (
                "Please specify if new routine is a UDF or stored procedure"
                in result.output
            )
            assert result.exit_code == 1

    def test_create_udf(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/udf")
            result = runner.invoke(
                create,
                ["udf.test_udf", "--udf"],
                obj={"DEFAULT_PROJECT": "moz-fx-data-shared-prod"},
            )
            assert result.exit_code == 0
            assert os.listdir("sql/moz-fx-data-shared-prod/udf") == ["test_udf"]
            assert "udf.sql" in os.listdir("sql/moz-fx-data-shared-prod/udf/test_udf")
            assert "metadata.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/udf/test_udf"
            )

    def test_create_stored_procedure(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/proc")
            result = runner.invoke(
                create,
                ["proc.test", "--stored_procedure"],
                obj={"DEFAULT_PROJECT": "moz-fx-data-shared-prod"},
            )
            assert result.exit_code == 0
            assert os.listdir("sql/moz-fx-data-shared-prod/proc") == ["test"]
            assert "stored_procedure.sql" in os.listdir(
                "sql/moz-fx-data-shared-prod/proc/test"
            )
            assert "metadata.yaml" in os.listdir(
                "sql/moz-fx-data-shared-prod/proc/test"
            )

    def test_create_mozfun_udf(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod")
            os.makedirs("sql/mozfun")
            result = runner.invoke(
                create,
                ["test_dataset.test_udf", "--udf"],
                obj={"DEFAULT_PROJECT": "mozfun"},
            )
            assert result.exit_code == 0
            assert os.listdir("sql/mozfun/test_dataset") == ["test_udf"]
            assert "udf.sql" in os.listdir("sql/mozfun/test_dataset/test_udf")
            assert "metadata.yaml" in os.listdir("sql/mozfun/test_dataset/test_udf")

    def test_routine_info(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/udf/test_udf")
            with open("sql/moz-fx-data-shared-prod/udf/test_udf/udf.sql", "w") as f:
                f.write("CREATE OR REPLACE FUNCTION udf.test_udf() AS (TRUE)")

            result = runner.invoke(
                info,
                ["udf.test_udf"],
                obj={"DEFAULT_PROJECT": "moz-fx-data-shared-prod"},
            )
            assert result.exit_code == 0
            assert "No metadata" in result.output
            assert "path:" in result.output

            metadata_conf = {"friendly_name": "test", "description": "test"}

            with open(
                "sql/moz-fx-data-shared-prod/udf/test_udf/metadata.yaml", "w"
            ) as f:
                f.write(yaml.dump(metadata_conf))

            result = runner.invoke(
                info,
                ["udf.test_udf"],
                obj={"DEFAULT_PROJECT": "moz-fx-data-shared-prod"},
            )
            assert result.exit_code == 0
            assert "No metadata" not in result.output
            assert "description" in result.output

    def test_udf_info_name_pattern(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/udf/test_udf")
            with open("sql/moz-fx-data-shared-prod/udf/test_udf/udf.sql", "w") as f:
                f.write("CREATE OR REPLACE FUNCTION udf.test_udf() AS (TRUE)")

            os.mkdir("sql/moz-fx-data-shared-prod/udf/another_udf")
            with open("sql/moz-fx-data-shared-prod/udf/another_udf/udf.sql", "w") as f:
                f.write("CREATE OR REPLACE FUNCTION udf.test_udf() AS (TRUE)")

            result = runner.invoke(
                info, ["udf.*"], obj={"DEFAULT_PROJECT": "moz-fx-data-shared-prod"}
            )
            assert result.exit_code == 0
            assert "udf.another_udf" in result.output
            assert "udf.test_udf" in result.output

            result = runner.invoke(
                info,
                ["udf.another*"],
                obj={"DEFAULT_PROJECT": "moz-fx-data-shared-prod"},
            )
            assert result.exit_code == 0
            assert "udf.another_udf" in result.output
            assert "udf.test_udf" not in result.output

    def test_udf_renaming_invalid_naming(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/udf/test_udf")
            with open("sql/moz-fx-data-shared-prod/udf/test_udf/udf.sql", "w") as f:
                f.write("CREATE OR REPLACE FUNCTION udf.test_udf() AS (TRUE)")

            os.mkdir("sql/moz-fx-data-shared-prod/udf/another_udf")
            with open("sql/moz-fx-data-shared-prod/udf/test_udf/udf.sql", "w") as f:
                f.write(
                    "CREATE OR REPLACE FUNCTION udf.another_udf() AS (udf.test_udf())"
                )

            result = runner.invoke(
                rename,
                ["udf.*", "something.else"],
                obj={"DEFAULT_PROJECT": "moz-fx-data-shared-prod"},
            )
            assert result.exit_code == 0

            result = runner.invoke(rename, ["dataset.udf"])
            assert result.exit_code == 2

    def test_udf_renaming(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/moz-fx-data-shared-prod/udf/test_udf")
            with open("sql/moz-fx-data-shared-prod/udf/test_udf/udf.sql", "w") as f:
                f.write("CREATE OR REPLACE FUNCTION udf.test_udf() AS (TRUE)")

            os.mkdir("sql/moz-fx-data-shared-prod/udf/another_udf")
            with open("sql/moz-fx-data-shared-prod/udf/another_udf/udf.sql", "w") as f:
                f.write(
                    "CREATE OR REPLACE FUNCTION udf.another_udf() AS (udf.test_udf())"
                )

            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT udf.test_udf()")

            result = runner.invoke(
                rename,
                ["udf.test_udf", "udf.renamed_udf"],
                obj={"DEFAULT_PROJECT": "moz-fx-data-shared-prod"},
            )
            assert result.exit_code == 0
            assert "test_udf" not in os.listdir("sql/moz-fx-data-shared-prod/udf")
            assert "another_udf" in os.listdir("sql/moz-fx-data-shared-prod/udf")
            assert "renamed_udf" in os.listdir("sql/moz-fx-data-shared-prod/udf")

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "r"
            ) as f:
                sql = f.read()
                assert "udf.renamed_udf" in sql
                assert "udf.test_udf" not in sql

            with open("sql/moz-fx-data-shared-prod/udf/another_udf/udf.sql", "r") as f:
                sql = f.read()
                assert "udf.renamed_udf" in sql
                assert "udf.test_udf" not in sql

            with open("sql/moz-fx-data-shared-prod/udf/renamed_udf/udf.sql", "r") as f:
                sql = f.read()
                assert "udf.renamed_udf" in sql
                assert "udf.test_udf" not in sql

    def test_mozfun_dataset_renaming(self, runner):
        with runner.isolated_filesystem():
            os.makedirs("sql/mozfun/udf/test_udf")
            with open("sql/mozfun/udf/test_udf/udf.sql", "w") as f:
                f.write("CREATE OR REPLACE FUNCTION udf.test_udf() AS (TRUE)")

            os.mkdir("sql/mozfun/udf/another_udf")
            with open("sql/mozfun/udf/another_udf/udf.sql", "w") as f:
                f.write(
                    "CREATE OR REPLACE FUNCTION udf.another_udf() AS (udf.test_udf())"
                )

            os.makedirs("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT udf.test_udf()")

            result = runner.invoke(
                rename, ["udf.*", "new_dataset"], obj={"DEFAULT_PROJECT": "mozfun"}
            )
            assert result.exit_code == 0
            assert "udf" not in os.listdir("sql/mozfun")
            assert "new_dataset" in os.listdir("sql/mozfun")
            assert "another_udf" in os.listdir("sql/mozfun/new_dataset")
            assert "test_udf" in os.listdir("sql/mozfun/new_dataset")

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "r"
            ) as f:
                sql = f.read()
                assert "new_dataset.test_udf" in sql
                assert "udf.test_udf" not in sql

            with open("sql/mozfun/new_dataset/another_udf/udf.sql", "r") as f:
                sql = f.read()
                assert "new_dataset.test_udf" in sql
                assert "udf.test_udf" not in sql

            with open("sql/mozfun/new_dataset/test_udf/udf.sql", "r") as f:
                sql = f.read()
                assert "new_dataset.test_udf" in sql
                assert "udf.test_udf" not in sql
