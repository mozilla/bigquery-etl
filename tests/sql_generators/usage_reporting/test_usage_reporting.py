from unittest.mock import patch

from sql_generators.usage_reporting.usage_reporting import (
    get_specific_apps_app_info_from_probe_scraper,
)

PROBE_SCRAPER_APP_INFO_MOCK_VALUE = {
    "firefox_desktop": [
        {
            "app_description": "The desktop version of Firefox",
            "app_id": "firefox.desktop",
            "app_name": "firefox_desktop",
            "bq_dataset_family": "firefox_desktop",
            "canonical_app_name": "Firefox for Desktop",
            "dependencies": [
                "gecko",
                "glean-core",
                "org.mozilla.components:service-glean",
            ],
            "document_namespace": "firefox-desktop",
            "metrics_files": ["browser/components/asrouter/metrics.yaml"],
            "moz_pipeline_metadata": {
                "baseline": {"expiration_policy": {"delete_after_days": 775}}
            },
            "ping_files": ["browser/components/asrouter/pings.yaml"],
            "tag_files": ["toolkit/components/glean/tags.yaml"],
            "url": "https://github.com/mozilla/gecko-dev",
            "v1_name": "firefox-desktop",
        }
    ],
    "fenix": [
        {
            "app_channel": "release",
            "app_description": "Firefox for Android (Fenix)",
            "app_id": "org.mozilla.firefox",
            "app_name": "fenix",
            "bq_dataset_family": "org_mozilla_firefox",
            "canonical_app_name": "Firefox for Android",
            "dependencies": ["gecko"],
            "description": "Release channel of Firefox for Android.",
            "document_namespace": "org-mozilla-firefox",
            "metrics_files": ["mobile/android/fenix/app/metrics.yaml"],
            "ping_files": ["mobile/android/fenix/app/pings.yaml"],
            "tag_files": ["mobile/android/fenix/app/tags.yaml"],
            "url": "https://github.com/mozilla/gecko-dev",
        },
        {
            "app_channel": "beta",
            "app_description": "Firefox for Android (Fenix)",
            "app_id": "org.mozilla.firefox_beta",
            "app_name": "fenix",
            "bq_dataset_family": "org_mozilla_firefox_beta",
            "canonical_app_name": "Firefox for Android",
        },
        {
            "app_channel": "nightly",
            "app_description": "Firefox for Android (Fenix)",
            "app_id": "org.mozilla.fenix",
            "app_name": "fenix",
            "bq_dataset_family": "org_mozilla_fenix",
        },
        {
            "app_channel": "nightly",
            "app_description": "Firefox for Android (Fenix)",
            "app_id": "org.mozilla.fenix.nightly",
            "app_name": "fenix",
            "bq_dataset_family": "org_mozilla_fenix_nightly",
            "canonical_app_name": "Firefox for Android",
            "dependencies": [
                "gecko",
                "glean-core",
                "org.mozilla.components:service-glean",
            ],
            "ping_files": ["mobile/android/fenix/app/pings.yaml"],
            "tag_files": ["mobile/android/fenix/app/tags.yaml"],
            "url": "https://github.com/mozilla/gecko-dev",
            "v1_name": "fenix-nightly",
        },
        {
            "app_channel": "nightly",
            "app_description": "Firefox for Android (Fenix)",
            "app_id": "org.mozilla.fennec.aurora",
            "app_name": "fenix",
            "bq_dataset_family": "org_mozilla_fennec_aurora",
            "canonical_app_name": "Firefox for Android",
        },
    ],
    "firefox_ios": [
        {
            "app_channel": "release",
            "app_description": "Firefox for iOS",
            "app_id": "org.mozilla.ios.Firefox",
            "app_name": "firefox_ios",
            "bq_dataset_family": "org_mozilla_ios_firefox",
            "branch": "main",
            "canonical_app_name": "Firefox for iOS",
        },
        {
            "app_channel": "beta",
            "app_description": "Firefox for iOS",
            "app_id": "org.mozilla.ios.FirefoxBeta",
            "app_name": "firefox_ios",
            "bq_dataset_family": "org_mozilla_ios_firefoxbeta",
            "branch": "main",
            "canonical_app_name": "Firefox for iOS",
            "dependencies": [
                "glean-core",
                "org.mozilla.components:service-glean",
                "nimbus",
                "org.mozilla.appservices:logins",
                "org.mozilla.appservices:syncmanager",
            ],
            "description": "Beta channel of Firefox for iOS.",
        },
        {
            "app_channel": "nightly",
            "app_description": "Firefox for iOS",
            "app_id": "org.mozilla.ios.Fennec",
            "app_name": "firefox_ios",
            "bq_dataset_family": "org_mozilla_ios_fennec",
        },
    ],
    "focus_ios": [
        {
            "app_description": "Firefox Focus on iOS. Klar is the sibling application",
            "app_id": "org.mozilla.ios.Focus",
            "app_name": "focus_ios",
            "bq_dataset_family": "org_mozilla_ios_focus",
            "branch": "main",
            "canonical_app_name": "Firefox Focus for iOS",
            "dependencies": ["glean-core", "nimbus"],
            "document_namespace": "org-mozilla-ios-focus",
            "metrics_files": ["focus-ios/Blockzilla/metrics.yaml"],
            "moz_pipeline_metadata_defaults": {
                "expiration_policy": {"delete_after_days": 720}
            },
            "notification_emails": ["sarentz@mozilla.com", "tlong@mozilla.com"],
            "ping_files": ["focus-ios/Blockzilla/pings.yaml"],
            "url": "https://github.com/mozilla-mobile/firefox-ios",
            "v1_name": "firefox-focus-ios",
        },
    ],
    "focus_android": [
        {
            "app_channel": "release",
            "app_name": "focus_android",
            "bq_dataset_family": "org_mozilla_focus",
        },
        {
            "app_channel": "beta",
            "app_name": "focus_android",
            "bq_dataset_family": "org_mozilla_focus_beta",
        },
        {
            "app_channel": "nightly",
            "app_name": "focus_android",
            "bq_dataset_family": "org_mozilla_focus_nightly",
        },
    ],
}


# TODO: update the test_get_specific_apps_app_info_from_probe_scraper tests to use paremetisation.
@patch("sql_generators.usage_reporting.usage_reporting.get_app_info")
def test_get_specific_apps_app_info_from_probe_scraper_empty(mock_get_app_info):
    mock_get_app_info.return_value = PROBE_SCRAPER_APP_INFO_MOCK_VALUE

    input = {}
    expected = {}

    actual = get_specific_apps_app_info_from_probe_scraper(input)

    assert mock_get_app_info.called
    assert expected == actual


# TODO: we should mock the response from probe scraper API in this test.
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
