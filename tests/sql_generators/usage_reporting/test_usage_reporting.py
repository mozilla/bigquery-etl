from pathlib import Path
from unittest.mock import patch

import yaml

from sql_generators.usage_reporting.usage_reporting import (
    get_specific_apps_app_info_from_probe_scraper,
)

TEST_DIR = Path(__file__).parent
TEST_CONFIG_FILE = "test_config.yaml"
PROBE_SCRAPER_APP_INFO_MOCK_VALUE = yaml.safe_load(
    (TEST_DIR / TEST_CONFIG_FILE).read_text()
)["probe_scraper_app_info_mock_response"]


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
