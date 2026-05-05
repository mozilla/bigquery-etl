from filecmp import cmp
from os import listdir
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from pytest import mark

from bigquery_etl.config import _ConfigLoader
from sql_generators.usage_reporting.usage_reporting import (
    generate_usage_reporting,
    get_specific_apps_app_info_from_probe_scraper,
    remove_table_version_suffix,
)

TEST_DIR = Path(__file__).parent
TEST_CONFIG_FILE = "test_config.yaml"

CONFIG_LOADER = _ConfigLoader()
CONFIG_LOADER.set_project_dir(TEST_DIR)
CONFIG_LOADER.set_config_file(TEST_CONFIG_FILE)

PROBE_SCRAPER_APP_INFO_MOCK_VALUE = CONFIG_LOADER.get(
    "probe_scraper_app_info_mock_response"
)


@patch("sql_generators.usage_reporting.usage_reporting.get_app_info")
def test_get_specific_apps_app_info_from_probe_scraper_empty(mock_get_app_info):
    mock_get_app_info.return_value = PROBE_SCRAPER_APP_INFO_MOCK_VALUE

    input = {}
    expected = {}

    actual = get_specific_apps_app_info_from_probe_scraper(input)

    assert mock_get_app_info.called
    assert expected == actual


@patch("sql_generators.usage_reporting.usage_reporting.get_app_info")
def test_get_specific_apps_app_info_from_probe_scraper(mock_get_app_info):
    mock_get_app_info.return_value = PROBE_SCRAPER_APP_INFO_MOCK_VALUE

    input = {
        "fenix": {"channels": ["nightly", "beta", "release"]},
        "firefox_ios": {"channels": ["nightly", "beta", "release"]},
        "firefox_desktop": {"channels": None},
        "focus_android": {"channels": ["release", "beta", "nightly"]},
        "focus_ios": {"channels": ["release"]},
    }

    expected = {
        "firefox_desktop": {
            "multichannel": {
                "app_channel": None,
                "app_name": "firefox_desktop",
                "bq_dataset_family": "firefox_desktop",
            }
        },
        "fenix": {
            "release__0": {
                "app_channel": "release",
                "app_name": "fenix",
                "bq_dataset_family": "org_mozilla_firefox",
            },
            "beta__1": {
                "app_channel": "beta",
                "app_name": "fenix",
                "bq_dataset_family": "org_mozilla_firefox_beta",
            },
            "nightly__2": {
                "app_channel": "nightly",
                "app_name": "fenix",
                "bq_dataset_family": "org_mozilla_fenix",
            },
            "nightly__3": {
                "app_channel": "nightly",
                "app_name": "fenix",
                "bq_dataset_family": "org_mozilla_fenix_nightly",
            },
            "nightly__4": {
                "app_channel": "nightly",
                "app_name": "fenix",
                "bq_dataset_family": "org_mozilla_fennec_aurora",
            },
        },
        "firefox_ios": {
            "release__0": {
                "app_channel": "release",
                "app_name": "firefox_ios",
                "bq_dataset_family": "org_mozilla_ios_firefox",
            },
            "beta__1": {
                "app_channel": "beta",
                "app_name": "firefox_ios",
                "bq_dataset_family": "org_mozilla_ios_firefoxbeta",
            },
            "nightly__2": {
                "app_channel": "nightly",
                "app_name": "firefox_ios",
                "bq_dataset_family": "org_mozilla_ios_fennec",
            },
        },
        "focus_ios": {
            "release__0": {
                "app_channel": "release",
                "app_name": "focus_ios",
                "bq_dataset_family": "org_mozilla_ios_focus",
            }
        },
        "focus_android": {
            "release__0": {
                "app_channel": "release",
                "app_name": "focus_android",
                "bq_dataset_family": "org_mozilla_focus",
            },
            "beta__1": {
                "app_channel": "beta",
                "app_name": "focus_android",
                "bq_dataset_family": "org_mozilla_focus_beta",
            },
            "nightly__2": {
                "app_channel": "nightly",
                "app_name": "focus_android",
                "bq_dataset_family": "org_mozilla_focus_nightly",
            },
        },
    }

    actual = get_specific_apps_app_info_from_probe_scraper(input)

    assert mock_get_app_info.called
    assert expected == actual


def helper_get_list_delta(_list: list, other_list: list) -> list:
    list_set = set(_list)
    other_list_set = set(other_list)

    return list(other_list_set.symmetric_difference(list_set))


@patch("sql_generators.usage_reporting.usage_reporting.get_app_info")
def test_get_specific_apps_app_info_from_probe_scraper_filtered(mock_get_app_info):
    mock_get_app_info.return_value = PROBE_SCRAPER_APP_INFO_MOCK_VALUE

    input = {
        "test": {"channels": None},
        "firefox_ios": {"channels": ["nightly"]},
        "firefox_desktop": {"channels": None},
    }
    actual = get_specific_apps_app_info_from_probe_scraper(input)

    expected = {
        "firefox_desktop": {
            "multichannel": {
                "app_channel": None,
                "app_name": "firefox_desktop",
                "bq_dataset_family": "firefox_desktop",
            }
        },
        "firefox_ios": {
            "nightly__2": {
                "app_channel": "nightly",
                "app_name": "firefox_ios",
                "bq_dataset_family": "org_mozilla_ios_fennec",
            },
        },
    }

    assert mock_get_app_info.called
    assert expected == actual


@patch("sql_generators.usage_reporting.usage_reporting.get_generation_config")
@patch("sql_generators.usage_reporting.usage_reporting.get_app_info")
def test_content_generated_as_expected(mock_get_app_info, mock_generation_config):
    project_id = "moz-fx-data-shared-prod"
    test_generate_input = {
        "fenix": {"channels": ["beta", "release"]},
        "firefox_ios": {"channels": ["beta"]},
        "firefox_desktop": {"channels": None},
    }

    mock_get_app_info.return_value = PROBE_SCRAPER_APP_INFO_MOCK_VALUE
    mock_generation_config.return_value = test_generate_input

    with TemporaryDirectory() as temp_dir:
        generate_usage_reporting(target_project=project_id, output_dir=temp_dir)

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

    assert mock_get_app_info.called
    assert mock_generation_config.called


@mark.parametrize(
    "test_input,expected",
    [
        ("my_tiny_dataset_v1", "my_tiny_dataset"),
        ("my_tiny_dataset_v11", "my_tiny_dataset"),
        ("my_tiny_dataset_v", "my_tiny_dataset"),
        ("my_tiny_dataset", "my_tiny_dataset"),
        (
            "gcp_project.dataset_id.my_tiny_dataset",
            "gcp_project.dataset_id.my_tiny_dataset",
        ),
        (
            "gcp_project.dataset_id.my_tiny_dataset_v10",
            "gcp_project.dataset_id.my_tiny_dataset",
        ),
    ],
)
def test_remove_table_version_suffix(test_input, expected):
    actual = remove_table_version_suffix(test_input)

    assert expected == actual
