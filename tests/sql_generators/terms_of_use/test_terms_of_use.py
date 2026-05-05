from filecmp import cmp
from os import listdir
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from bigquery_etl.config import _ConfigLoader
from sql_generators.terms_of_use.terms_of_use import generate_terms_of_use

TEST_DIR = Path(__file__).parent
TEST_CONFIG_FILE = "test_config.yaml"

CONFIG_LOADER = _ConfigLoader()
CONFIG_LOADER.set_project_dir(TEST_DIR)
CONFIG_LOADER.set_config_file(TEST_CONFIG_FILE)

CONFIG_MOCK_RESPONSE = CONFIG_LOADER.get("generate", "terms_of_use")


def helper_get_list_delta(_list: list, other_list: list) -> list:
    list_set = set(_list)
    other_list_set = set(other_list)

    return list(other_list_set.symmetric_difference(list_set))


@patch("sql_generators.terms_of_use.terms_of_use.get_generation_config")
def test_content_generated_as_expected(mock_generation_config):
    project_id = "moz-fx-data-shared-prod"
    mock_generation_config.return_value = CONFIG_MOCK_RESPONSE

    with TemporaryDirectory() as temp_dir:
        generate_terms_of_use(target_project=project_id, output_dir=temp_dir)

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

    assert mock_generation_config.called
