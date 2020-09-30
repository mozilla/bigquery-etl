import os
import pytest
from click.testing import CliRunner
import yaml

from bigquery_etl.cli.udf import create, info, rename


class TestUdf:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_create_invalid_path(self, runner):
        with runner.isolated_filesystem():
            with open("foo.txt", "w") as f:
                f.write("")
            result = runner.invoke(create, ["udf.test_udf", "--path=foo.txt"])
            assert result.exit_code == 2

    def test_create_invalid_udf_name(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("udf")
            result = runner.invoke(
                create, ["udf.udf.test_udf"], obj={"UDF_DIRS": ("udf",)}
            )
            assert result.exit_code == 1

    def test_create_udf(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("udf")
            result = runner.invoke(create, ["udf.test_udf"], obj={"UDF_DIRS": ("udf",)})
            assert result.exit_code == 0
            assert os.listdir("udf") == ["test_udf"]
            assert "udf.sql" in os.listdir("udf/test_udf")
            assert "metadata.yaml" in os.listdir("udf/test_udf")

    def test_create_udf_with_path(self, runner):
        with runner.isolated_filesystem():
            udf_path = "udf_alt"
            os.mkdir(udf_path)
            result = runner.invoke(
                create, ["-p", udf_path, "udf.test_udf"], obj={"UDF_DIRS": (udf_path,)}
            )
            assert result.exit_code == 0
            assert os.listdir(udf_path) == ["udf"]
            assert os.listdir(f"{udf_path}/udf") == ["test_udf"]
            assert "udf.sql" in os.listdir(f"{udf_path}/udf/test_udf")
            assert "metadata.yaml" in os.listdir(f"{udf_path}/udf/test_udf")

    def test_create_mozfun_udf(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("mozfun")
            result = runner.invoke(
                create, ["test_dataset.test_udf"], obj={"UDF_DIRS": ("mozfun",)}
            )
            assert result.exit_code == 0
            assert os.listdir("mozfun/test_dataset") == ["test_udf"]
            assert "udf.sql" in os.listdir("mozfun/test_dataset/test_udf")
            assert "metadata.yaml" in os.listdir("mozfun/test_dataset/test_udf")

    def test_udf_info(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("udf")
            os.mkdir("udf/test_udf")
            with open("udf/test_udf/udf.sql", "w") as f:
                f.write("CREATE OR REPLACE FUNCTION udf.test_udf() AS (TRUE)")

            result = runner.invoke(info, ["udf.test_udf"], obj={"UDF_DIRS": ("udf",)})
            assert result.exit_code == 0
            assert "No metadata" in result.output
            assert "path:" in result.output

            metadata_conf = {"friendly_name": "test", "description": "test"}

            with open("udf/test_udf/metadata.yaml", "w") as f:
                f.write(yaml.dump(metadata_conf))

            result = runner.invoke(info, ["udf.test_udf"], obj={"UDF_DIRS": ("udf",)})
            assert result.exit_code == 0
            assert "No metadata" not in result.output
            assert "description" in result.output

    def test_udf_info_name_pattern(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("udf")
            os.mkdir("udf/test_udf")
            with open("udf/test_udf/udf.sql", "w") as f:
                f.write("CREATE OR REPLACE FUNCTION udf.test_udf() AS (TRUE)")

            os.mkdir("udf/another_udf")
            with open("udf/another_udf/udf.sql", "w") as f:
                f.write("CREATE OR REPLACE FUNCTION udf.test_udf() AS (TRUE)")

            result = runner.invoke(info, ["udf.*"], obj={"UDF_DIRS": ("udf",)})
            assert result.exit_code == 0
            assert "udf.another_udf" in result.output
            assert "udf.test_udf" in result.output

            result = runner.invoke(info, ["udf.another*"], obj={"UDF_DIRS": ("udf",)})
            assert result.exit_code == 0
            assert "udf.another_udf" in result.output
            assert "udf.test_udf" not in result.output

    def test_udf_renaming_invalid_naming(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("udf")
            os.mkdir("udf/test_udf")
            with open("udf/test_udf/udf.sql", "w") as f:
                f.write("CREATE OR REPLACE FUNCTION udf.test_udf() AS (TRUE)")

            os.mkdir("udf/another_udf")
            with open("udf/another_udf/udf.sql", "w") as f:
                f.write(
                    "CREATE OR REPLACE FUNCTION udf.another_udf() AS (udf.test_udf())"
                )

            result = runner.invoke(
                rename, ["udf.*", "something.else"], obj={"UDF_DIRS": ("udf",)}
            )
            assert result.exit_code == 1

            result = runner.invoke(rename, ["dataset.udf"])
            assert result.exit_code == 2

    def test_udf_renaming(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("udf")
            os.mkdir("udf/test_udf")
            with open("udf/test_udf/udf.sql", "w") as f:
                f.write("CREATE OR REPLACE FUNCTION udf.test_udf() AS (TRUE)")

            os.mkdir("udf/another_udf")
            with open("udf/another_udf/udf.sql", "w") as f:
                f.write(
                    "CREATE OR REPLACE FUNCTION udf.another_udf() AS (udf.test_udf())"
                )

            os.mkdir("sql")
            os.mkdir("sql/moz-fx-data-shared-prod")
            os.mkdir("sql/moz-fx-data-shared-prod/telemetry_derived")
            os.mkdir("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT udf.test_udf()")

            result = runner.invoke(
                rename,
                ["udf.test_udf", "udf.renamed_udf"],
                obj={"UDF_DIRS": ("udf",)},
            )
            assert result.exit_code == 0
            assert "test_udf" not in os.listdir("udf")
            assert "another_udf" in os.listdir("udf")
            assert "renamed_udf" in os.listdir("udf")

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "r"
            ) as f:
                sql = f.read()
                assert "udf.renamed_udf" in sql
                assert "udf.test_udf" not in sql

            with open("udf/another_udf/udf.sql", "r") as f:
                sql = f.read()
                assert "udf.renamed_udf" in sql
                assert "udf.test_udf" not in sql

            with open("udf/renamed_udf/udf.sql", "r") as f:
                sql = f.read()
                assert "udf.renamed_udf" in sql
                assert "udf.test_udf" not in sql

    def test_mozfun_dataset_renaming(self, runner):
        with runner.isolated_filesystem():
            os.mkdir("mozfun")
            os.mkdir("mozfun/udf")
            os.mkdir("mozfun/udf/test_udf")
            with open("mozfun/udf/test_udf/udf.sql", "w") as f:
                f.write("CREATE OR REPLACE FUNCTION udf.test_udf() AS (TRUE)")

            os.mkdir("mozfun/udf/another_udf")
            with open("mozfun/udf/another_udf/udf.sql", "w") as f:
                f.write(
                    "CREATE OR REPLACE FUNCTION udf.another_udf() AS (udf.test_udf())"
                )

            os.mkdir("sql")
            os.mkdir("sql/moz-fx-data-shared-prod")
            os.mkdir("sql/moz-fx-data-shared-prod/telemetry_derived")
            os.mkdir("sql/moz-fx-data-shared-prod/telemetry_derived/query_v1")
            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "w"
            ) as f:
                f.write("SELECT udf.test_udf()")

            result = runner.invoke(
                rename, ["udf.*", "new_dataset"], obj={"UDF_DIRS": ("mozfun",)}
            )
            assert result.exit_code == 0
            assert "udf" not in os.listdir("mozfun")
            assert "new_dataset" in os.listdir("mozfun")
            assert "another_udf" in os.listdir("mozfun/new_dataset")
            assert "test_udf" in os.listdir("mozfun/new_dataset")

            with open(
                "sql/moz-fx-data-shared-prod/telemetry_derived/query_v1/query.sql", "r"
            ) as f:
                sql = f.read()
                assert "new_dataset.test_udf" in sql
                assert "udf.test_udf" not in sql

            with open("mozfun/new_dataset/another_udf/udf.sql", "r") as f:
                sql = f.read()
                assert "new_dataset.test_udf" in sql
                assert "udf.test_udf" not in sql

            with open("mozfun/new_dataset/test_udf/udf.sql", "r") as f:
                sql = f.read()
                assert "new_dataset.test_udf" in sql
                assert "udf.test_udf" not in sql
