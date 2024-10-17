import filecmp
import glob
from tempfile import TemporaryDirectory

import pytest
from click.testing import CliRunner

from sql_generators.mobile_kpi_support_metrics import MobileProducts, generate


class TestMobileKpiSupportMetricsGenerate:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_generate(self, runner):
        project = "moz-fx-data-shared-prod"
        test_dir = "tests/sql_generators/mobile_kpi_support_metrics"

        with TemporaryDirectory() as tmp_dir:
            result = runner.invoke(generate, [f"--output-dir={tmp_dir}/actual"])

            assert result.exit_code == 0

            deltas = list()

            for product in MobileProducts:
                expected_model_paths = glob.glob(
                    f"{test_dir}/expected/{project}/{product.name}*/*"
                )

                for expected_model_path in expected_model_paths:
                    model_name = expected_model_path.split("/")[-2:]

                    actual_model_path = glob.glob(
                        f"{tmp_dir}/actual/{project}/{'/'.join(model_name)}"
                    )

                    dcmp = filecmp.dircmp(expected_model_path, actual_model_path[0])

                    for diff_files in dcmp.diff_files:
                        if diff_files:
                            deltas.append(
                                (
                                    expected_model_path,
                                    actual_model_path[0],
                                    diff_files,
                                )
                            )

            if deltas:
                print(*deltas, sep="\n")

            # assert len(deltas) == 0
