from enum import Enum
from filecmp import cmp
from os import listdir
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from bigquery_etl.config import _ConfigLoader
from sql_generators.mobile_kpi_support_metrics.mobile_kpi_support_metrics import (
    AttributionFields,
    Product,
    generate_mobile_kpi_support_metrics,
)

TEST_DIR = Path(__file__).parent
TEST_CONFIG_FILE = "test_config.yaml"

CONFIG_LOADER = _ConfigLoader()
CONFIG_LOADER.set_project_dir(TEST_DIR)
CONFIG_LOADER.set_config_file(TEST_CONFIG_FILE)


class MobileTestProducts(Enum):
    """Enumeration with browser names and equivalent dataset names."""

    fenix = Product(
        friendly_name="Fenix",
        search_join_key="Firefox Android",
        is_mobile_kpi=True,
        enable_monitoring=True,
        attribution_groups=[
            AttributionFields.play_store,
            AttributionFields.meta,
            AttributionFields.install_source,
            AttributionFields.adjust,
            AttributionFields.distribution_id,
        ],
    )
    focus_android = Product(
        friendly_name="Focus Android",
        search_join_key="Focus Android",
        is_mobile_kpi=True,
    )
    klar_android = Product(
        friendly_name="Klar Android",
        search_join_key="Klar Android",
    )
    firefox_ios = Product(
        friendly_name="Firefox iOS",
        search_join_key="Firefox iOS",
        is_mobile_kpi=True,
        enable_monitoring=True,
        attribution_groups=[
            AttributionFields.is_suspicious_device_client,
            AttributionFields.adjust,
        ],
    )


def helper_get_list_delta(_list: list, other_list: list) -> list:
    return list(set(other_list).symmetric_difference(set(_list)))


def test_content_generated_as_expected():
    project_id = "moz-fx-data-shared-prod"

    with TemporaryDirectory() as temp_dir:
        with patch(
            "sql_generators.mobile_kpi_support_metrics.mobile_kpi_support_metrics.MobileProducts",
            MobileTestProducts,
        ):
            generate_mobile_kpi_support_metrics(
                target_project=project_id, output_dir=temp_dir
            )

        expected_folder_structure = listdir(f"{TEST_DIR}/expected/{project_id}")
        generated_folder_structure = listdir(f"{temp_dir}/{project_id}")

        directory_delta = helper_get_list_delta(
            expected_folder_structure, generated_folder_structure
        )

        assert len(directory_delta) == 0

        for expected_folder in expected_folder_structure:
            expected_sql_directories = listdir(
                f"{TEST_DIR}/expected/{project_id}/{expected_folder}"
            )
            generated_sql_directories = listdir(
                f"{temp_dir}/{project_id}/{expected_folder}"
            )

            sql_directories_delta = helper_get_list_delta(
                expected_sql_directories, generated_sql_directories
            )

            assert len(sql_directories_delta) == 0

            for sql_directory in expected_sql_directories:
                expected_files = listdir(
                    f"{TEST_DIR}/expected/{project_id}/{expected_folder}/{sql_directory}"
                )
                generated_files = listdir(
                    f"{temp_dir}/{project_id}/{expected_folder}/{sql_directory}"
                )

                generated_files_delta = helper_get_list_delta(
                    expected_files, generated_files
                )

                assert len(generated_files_delta) == 0

                for _file in expected_files:
                    expected_file = f"{TEST_DIR}/expected/{project_id}/{expected_folder}/{sql_directory}/{_file}"
                    generated_file = f"{temp_dir}/{project_id}/{expected_folder}/{sql_directory}/{_file}"

                    assert cmp(expected_file, generated_file)
