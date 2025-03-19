from sql_generators.usage_reporting.usage_reporting import (
    get_specific_apps_app_info_from_probe_scraper,
)

# TODO: update the test_get_specific_apps_app_info_from_probe_scraper tests to use paremetisation.


def test_get_specific_apps_app_info_from_probe_scraper_empty():
    expected = {}

    input = {}
    actual = get_specific_apps_app_info_from_probe_scraper(input)

    assert expected == actual


# TODO: we should mock the response from probe scraper API in this test.
def test_get_specific_apps_app_info_from_probe_scraper():
    expected = {
        "firefox_desktop": {
            "multichannel": {
                "app_channel": "release",
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
            "multichannel": {
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

    input = {
        "fenix": {"channels": ["nightly", "beta", "release"]},
        "firefox_ios": {"channels": ["nightly", "beta", "release"]},
        "firefox_desktop": {"channels": None},
        "focus_android": {"channels": ["release", "beta", "nightly"]},
        "focus_ios": {"channels": ["release"]},
    }
    actual = get_specific_apps_app_info_from_probe_scraper(input)

    assert expected == actual


# TODO: we should mock the response from probe scraper API in this test.
def test_get_specific_apps_app_info_from_probe_scraper_filtered():
    expected = {
        "firefox_desktop": {
            "multichannel": {
                "app_channel": "release",
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

    input = {
        "firefox_ios": {"channels": ["nightly"]},
        "firefox_desktop": {"channels": None},
    }
    actual = get_specific_apps_app_info_from_probe_scraper(input)

    assert expected == actual
