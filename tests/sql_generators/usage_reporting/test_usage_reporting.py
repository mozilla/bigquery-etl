from sql_generators.usage_reporting.usage_reporting import (
    get_specific_apps_app_info_from_probe_scraper,
)


def test_get_specific_apps_app_info_from_probe_scraper_empty():
    expected = {}

    input = {}
    actual = get_specific_apps_app_info_from_probe_scraper(input)

    assert expected == actual


def test_get_specific_apps_app_info_from_probe_scraper():
    expected = {
        "firefox_desktop": {
            "multichannel": {
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
                "metrics_files": [
                    "browser/components/asrouter/metrics.yaml",
                    "browser/components/backup/metrics.yaml",
                    "browser/components/doh/metrics.yaml",
                    "browser/components/downloads/metrics.yaml",
                    "browser/components/firefoxview/metrics.yaml",
                    "browser/components/genai/metrics.yaml",
                    "browser/components/metrics.yaml",
                    "browser/components/migration/metrics.yaml",
                    "browser/components/newtab/metrics.yaml",
                    "browser/components/places/metrics.yaml",
                    "browser/components/pocket/metrics.yaml",
                    "browser/components/preferences/metrics.yaml",
                    "browser/components/privatebrowsing/metrics.yaml",
                    "browser/components/profiles/metrics.yaml",
                    "browser/components/protections/metrics.yaml",
                    "browser/components/protocolhandler/metrics.yaml",
                    "browser/components/screenshots/metrics.yaml",
                    "browser/components/search/metrics.yaml",
                    "browser/components/sessionstore/metrics.yaml",
                    "browser/components/shopping/metrics.yaml",
                    "browser/components/sidebar/metrics.yaml",
                    "browser/components/tabbrowser/metrics.yaml",
                    "browser/components/textrecognition/metrics.yaml",
                    "browser/components/urlbar/metrics.yaml",
                    "browser/extensions/search-detection/metrics.yaml",
                    "browser/modules/metrics.yaml",
                    "dom/media/platforms/wmf/metrics.yaml",
                    "services/fxaccounts/metrics.yaml",
                    "toolkit/components/contentrelevancy/metrics.yaml",
                    "toolkit/components/crashes/metrics.yaml",
                    "toolkit/components/nimbus/metrics.yaml",
                    "toolkit/components/pictureinpicture/metrics.yaml",
                    "toolkit/components/places/metrics.yaml",
                    "toolkit/components/reportbrokensite/metrics.yaml",
                    "toolkit/components/satchel/megalist/metrics.yaml",
                    "toolkit/components/search/metrics.yaml",
                    "toolkit/components/shopping/metrics.yaml",
                    "toolkit/components/telemetry/metrics.yaml",
                    "toolkit/modules/metrics.yaml",
                    "toolkit/mozapps/update/shared_metrics.yaml",
                    "widget/cocoa/metrics.yaml",
                    "widget/gtk/metrics.yaml",
                    "widget/windows/metrics.yaml",
                ],
                "moz_pipeline_metadata": {
                    "baseline": {"expiration_policy": {"delete_after_days": 775}},
                    "crash": {"expiration_policy": {"delete_after_days": 775}},
                    "deletion-request": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "events": {"expiration_policy": {"delete_after_days": 775}},
                    "first-startup": {"expiration_policy": {"delete_after_days": 775}},
                    "fog-validation": {"expiration_policy": {"delete_after_days": 775}},
                    "fx-accounts": {"expiration_policy": {"delete_after_days": 30}},
                    "messaging-system": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "metrics": {"expiration_policy": {"delete_after_days": 775}},
                    "new-metric-capture-emulation": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "newtab": {"expiration_policy": {"delete_after_days": 775}},
                    "pageload": {"expiration_policy": {"delete_after_days": 400}},
                    "pseudo-main": {"expiration_policy": {"delete_after_days": 775}},
                    "quick-suggest": {
                        "expiration_policy": {"delete_after_days": 30},
                        "override_attributes": [{"name": "geo_city", "value": None}],
                        "submission_timestamp_granularity": "seconds",
                    },
                    "spoc": {"expiration_policy": {"delete_after_days": 180}},
                    "top-sites": {
                        "expiration_policy": {"delete_after_days": 30},
                        "override_attributes": [{"name": "geo_city", "value": None}],
                        "submission_timestamp_granularity": "seconds",
                    },
                    "user-characteristics": {
                        "expiration_policy": {"delete_after_days": 90}
                    },
                },
                "moz_pipeline_metadata_defaults": {
                    "expiration_policy": {"delete_after_days": 400}
                },
                "notification_emails": ["chutten@mozilla.com"],
                "ping_files": [
                    "browser/components/asrouter/pings.yaml",
                    "browser/components/newtab/pings.yaml",
                    "browser/components/pocket/pings.yaml",
                    "browser/components/search/pings.yaml",
                    "browser/components/urlbar/pings.yaml",
                    "browser/modules/pings.yaml",
                    "services/fxaccounts/pings.yaml",
                    "toolkit/components/crashes/pings.yaml",
                    "toolkit/components/nimbus/pings.yaml",
                    "toolkit/components/reportbrokensite/pings.yaml",
                    "toolkit/components/telemetry/pings.yaml",
                    "toolkit/modules/pings.yaml",
                ],
                "tag_files": ["toolkit/components/glean/tags.yaml"],
                "url": "https://github.com/mozilla/gecko-dev",
                "v1_name": "firefox-desktop",
            }
        },
        "fenix": {
            "release__0": {
                "app_channel": "release",
                "app_description": "Firefox for Android (Fenix)",
                "app_id": "org.mozilla.firefox",
                "app_name": "fenix",
                "bq_dataset_family": "org_mozilla_firefox",
                "canonical_app_name": "Firefox for Android",
                "dependencies": [
                    "gecko",
                    "glean-core",
                    "org.mozilla.components:service-glean",
                    "org.mozilla.components:lib-crash",
                    "org.mozilla.appservices:syncmanager",
                    "org.mozilla.appservices:logins",
                    "org.mozilla.components:support-migration",
                    "org.mozilla.components:places",
                    "org.mozilla.appservices:fxaclient",
                    "nimbus",
                    "org.mozilla.components:service-nimbus",
                    "org.mozilla.components:browser-engine-gecko",
                ],
                "description": "Release channel of Firefox for Android.",
                "document_namespace": "org-mozilla-firefox",
                "metrics_files": ["mobile/android/fenix/app/metrics.yaml"],
                "moz_pipeline_metadata": {
                    "activation": {"expiration_policy": {"delete_after_days": 775}},
                    "addresses-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "baseline": {"expiration_policy": {"delete_after_days": 775}},
                    "bookmarks-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "client-deduplication": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "cookie-banner-report-site": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "crash": {"expiration_policy": {"delete_after_days": 775}},
                    "creditcards-sync": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "deletion-request": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "events": {"expiration_policy": {"delete_after_days": 775}},
                    "first-session": {"expiration_policy": {"delete_after_days": 775}},
                    "fog-validation": {"expiration_policy": {"delete_after_days": 775}},
                    "history-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "installation": {"expiration_policy": {"delete_after_days": 775}},
                    "logins-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "metrics": {"expiration_policy": {"delete_after_days": 775}},
                    "migration": {"expiration_policy": {"delete_after_days": 775}},
                    "pageload": {"expiration_policy": {"delete_after_days": 400}},
                    "spoc": {"expiration_policy": {"delete_after_days": 180}},
                    "startup-timeline": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "sync": {"expiration_policy": {"delete_after_days": 775}},
                    "tabs-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "topsites-impression": {
                        "expiration_policy": {"delete_after_days": 30},
                        "override_attributes": [{"name": "geo_city", "value": None}],
                        "submission_timestamp_granularity": "seconds",
                    },
                    "user-characteristics": {
                        "expiration_policy": {"delete_after_days": 90}
                    },
                },
                "moz_pipeline_metadata_defaults": {
                    "expiration_policy": {"delete_after_days": 400}
                },
                "notification_emails": ["aplacitelli@mozilla.com"],
                "ping_files": ["mobile/android/fenix/app/pings.yaml"],
                "tag_files": ["mobile/android/fenix/app/tags.yaml"],
                "url": "https://github.com/mozilla/gecko-dev",
                "v1_name": "firefox-android-release",
            },
            "beta__1": {
                "app_channel": "beta",
                "app_description": "Firefox for Android (Fenix)",
                "app_id": "org.mozilla.firefox_beta",
                "app_name": "fenix",
                "bq_dataset_family": "org_mozilla_firefox_beta",
                "canonical_app_name": "Firefox for Android",
                "dependencies": [
                    "gecko",
                    "glean-core",
                    "org.mozilla.components:service-glean",
                    "org.mozilla.components:lib-crash",
                    "org.mozilla.appservices:syncmanager",
                    "org.mozilla.appservices:logins",
                    "org.mozilla.components:support-migration",
                    "org.mozilla.components:places",
                    "org.mozilla.appservices:fxaclient",
                    "nimbus",
                    "org.mozilla.components:service-nimbus",
                    "org.mozilla.components:browser-engine-gecko-beta",
                ],
                "description": "Beta channel of Firefox for Android.",
                "document_namespace": "org-mozilla-firefox-beta",
                "metrics_files": ["mobile/android/fenix/app/metrics.yaml"],
                "moz_pipeline_metadata": {
                    "activation": {"expiration_policy": {"delete_after_days": 775}},
                    "addresses-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "baseline": {"expiration_policy": {"delete_after_days": 775}},
                    "bookmarks-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "client-deduplication": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "cookie-banner-report-site": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "crash": {"expiration_policy": {"delete_after_days": 775}},
                    "creditcards-sync": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "deletion-request": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "events": {"expiration_policy": {"delete_after_days": 775}},
                    "first-session": {"expiration_policy": {"delete_after_days": 775}},
                    "fog-validation": {"expiration_policy": {"delete_after_days": 775}},
                    "history-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "installation": {"expiration_policy": {"delete_after_days": 775}},
                    "logins-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "metrics": {"expiration_policy": {"delete_after_days": 775}},
                    "migration": {"expiration_policy": {"delete_after_days": 775}},
                    "pageload": {"expiration_policy": {"delete_after_days": 400}},
                    "spoc": {"expiration_policy": {"delete_after_days": 180}},
                    "startup-timeline": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "sync": {"expiration_policy": {"delete_after_days": 775}},
                    "tabs-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "topsites-impression": {
                        "expiration_policy": {"delete_after_days": 30},
                        "override_attributes": [{"name": "geo_city", "value": None}],
                        "submission_timestamp_granularity": "seconds",
                    },
                    "user-characteristics": {
                        "expiration_policy": {"delete_after_days": 90}
                    },
                },
                "moz_pipeline_metadata_defaults": {
                    "expiration_policy": {"delete_after_days": 400}
                },
                "notification_emails": ["aplacitelli@mozilla.com"],
                "ping_files": ["mobile/android/fenix/app/pings.yaml"],
                "tag_files": ["mobile/android/fenix/app/tags.yaml"],
                "url": "https://github.com/mozilla/gecko-dev",
                "v1_name": "firefox-android-beta",
            },
            "nightly__2": {
                "app_channel": "nightly",
                "app_description": "Firefox for Android (Fenix)",
                "app_id": "org.mozilla.fenix",
                "app_name": "fenix",
                "bq_dataset_family": "org_mozilla_fenix",
                "canonical_app_name": "Firefox for Android",
                "dependencies": [
                    "gecko",
                    "glean-core",
                    "org.mozilla.components:service-glean",
                    "org.mozilla.components:lib-crash",
                    "org.mozilla.appservices:syncmanager",
                    "org.mozilla.appservices:logins",
                    "org.mozilla.components:support-migration",
                    "org.mozilla.components:places",
                    "org.mozilla.appservices:fxaclient",
                    "nimbus",
                    "org.mozilla.components:service-nimbus",
                    "org.mozilla.components:browser-engine-gecko-beta",
                ],
                "description": "Nightly channel of Firefox for Android. Prior to June 2020, this app_id was used for the beta channel of Firefox Preview.",
                "document_namespace": "org-mozilla-fenix",
                "metrics_files": ["mobile/android/fenix/app/metrics.yaml"],
                "moz_pipeline_metadata": {
                    "activation": {"expiration_policy": {"delete_after_days": 775}},
                    "addresses-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "baseline": {"expiration_policy": {"delete_after_days": 775}},
                    "bookmarks-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "client-deduplication": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "cookie-banner-report-site": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "crash": {"expiration_policy": {"delete_after_days": 775}},
                    "creditcards-sync": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "deletion-request": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "events": {"expiration_policy": {"delete_after_days": 775}},
                    "first-session": {"expiration_policy": {"delete_after_days": 775}},
                    "fog-validation": {"expiration_policy": {"delete_after_days": 775}},
                    "history-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "installation": {"expiration_policy": {"delete_after_days": 775}},
                    "logins-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "metrics": {"expiration_policy": {"delete_after_days": 775}},
                    "migration": {"expiration_policy": {"delete_after_days": 775}},
                    "pageload": {"expiration_policy": {"delete_after_days": 400}},
                    "spoc": {"expiration_policy": {"delete_after_days": 180}},
                    "startup-timeline": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "sync": {"expiration_policy": {"delete_after_days": 775}},
                    "tabs-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "topsites-impression": {
                        "expiration_policy": {"delete_after_days": 30},
                        "override_attributes": [{"name": "geo_city", "value": None}],
                        "submission_timestamp_granularity": "seconds",
                    },
                    "user-characteristics": {
                        "expiration_policy": {"delete_after_days": 90}
                    },
                },
                "moz_pipeline_metadata_defaults": {
                    "expiration_policy": {"delete_after_days": 400}
                },
                "notification_emails": ["aplacitelli@mozilla.com"],
                "ping_files": ["mobile/android/fenix/app/pings.yaml"],
                "tag_files": ["mobile/android/fenix/app/tags.yaml"],
                "url": "https://github.com/mozilla/gecko-dev",
                "v1_name": "fenix",
            },
            "nightly__3": {
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
                    "org.mozilla.components:lib-crash",
                    "org.mozilla.appservices:syncmanager",
                    "org.mozilla.appservices:logins",
                    "org.mozilla.components:support-migration",
                    "org.mozilla.components:places",
                    "org.mozilla.appservices:fxaclient",
                    "nimbus",
                    "org.mozilla.components:service-nimbus",
                    "org.mozilla.components:browser-engine-gecko-nightly",
                ],
                "description": "Nightly channel of Firefox Preview.",
                "document_namespace": "org-mozilla-fenix-nightly",
                "metrics_files": ["mobile/android/fenix/app/metrics.yaml"],
                "moz_pipeline_metadata": {
                    "activation": {"expiration_policy": {"delete_after_days": 775}},
                    "addresses-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "baseline": {"expiration_policy": {"delete_after_days": 775}},
                    "bookmarks-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "client-deduplication": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "cookie-banner-report-site": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "crash": {"expiration_policy": {"delete_after_days": 775}},
                    "creditcards-sync": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "deletion-request": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "events": {"expiration_policy": {"delete_after_days": 775}},
                    "first-session": {"expiration_policy": {"delete_after_days": 775}},
                    "fog-validation": {"expiration_policy": {"delete_after_days": 775}},
                    "history-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "installation": {"expiration_policy": {"delete_after_days": 775}},
                    "logins-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "metrics": {"expiration_policy": {"delete_after_days": 775}},
                    "migration": {"expiration_policy": {"delete_after_days": 775}},
                    "pageload": {"expiration_policy": {"delete_after_days": 400}},
                    "spoc": {"expiration_policy": {"delete_after_days": 180}},
                    "startup-timeline": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "sync": {"expiration_policy": {"delete_after_days": 775}},
                    "tabs-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "topsites-impression": {
                        "expiration_policy": {"delete_after_days": 30},
                        "override_attributes": [{"name": "geo_city", "value": None}],
                        "submission_timestamp_granularity": "seconds",
                    },
                    "user-characteristics": {
                        "expiration_policy": {"delete_after_days": 90}
                    },
                },
                "moz_pipeline_metadata_defaults": {
                    "expiration_policy": {"delete_after_days": 400}
                },
                "notification_emails": ["aplacitelli@mozilla.com"],
                "ping_files": ["mobile/android/fenix/app/pings.yaml"],
                "tag_files": ["mobile/android/fenix/app/tags.yaml"],
                "url": "https://github.com/mozilla/gecko-dev",
                "v1_name": "fenix-nightly",
            },
            "nightly__4": {
                "app_channel": "nightly",
                "app_description": "Firefox for Android (Fenix)",
                "app_id": "org.mozilla.fennec.aurora",
                "app_name": "fenix",
                "bq_dataset_family": "org_mozilla_fennec_aurora",
                "canonical_app_name": "Firefox for Android",
                "dependencies": [
                    "gecko",
                    "glean-core",
                    "org.mozilla.components:service-glean",
                    "org.mozilla.components:lib-crash",
                    "org.mozilla.appservices:syncmanager",
                    "org.mozilla.appservices:logins",
                    "org.mozilla.components:support-migration",
                    "org.mozilla.components:places",
                    "org.mozilla.appservices:fxaclient",
                    "nimbus",
                    "org.mozilla.components:service-nimbus",
                    "org.mozilla.components:browser-engine-gecko-beta",
                ],
                "description": "Nightly channel of Firefox for Android users migrated to Fenix; delisted in June 2020.",
                "document_namespace": "org-mozilla-fennec-aurora",
                "metrics_files": ["mobile/android/fenix/app/metrics.yaml"],
                "moz_pipeline_metadata": {
                    "activation": {"expiration_policy": {"delete_after_days": 775}},
                    "addresses-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "baseline": {"expiration_policy": {"delete_after_days": 775}},
                    "bookmarks-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "client-deduplication": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "cookie-banner-report-site": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "crash": {"expiration_policy": {"delete_after_days": 775}},
                    "creditcards-sync": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "deletion-request": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "events": {"expiration_policy": {"delete_after_days": 775}},
                    "first-session": {"expiration_policy": {"delete_after_days": 775}},
                    "fog-validation": {"expiration_policy": {"delete_after_days": 775}},
                    "history-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "installation": {"expiration_policy": {"delete_after_days": 775}},
                    "logins-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "metrics": {"expiration_policy": {"delete_after_days": 775}},
                    "migration": {"expiration_policy": {"delete_after_days": 775}},
                    "pageload": {"expiration_policy": {"delete_after_days": 400}},
                    "spoc": {"expiration_policy": {"delete_after_days": 180}},
                    "startup-timeline": {
                        "expiration_policy": {"delete_after_days": 775}
                    },
                    "sync": {"expiration_policy": {"delete_after_days": 775}},
                    "tabs-sync": {"expiration_policy": {"delete_after_days": 775}},
                    "topsites-impression": {
                        "expiration_policy": {"delete_after_days": 30},
                        "override_attributes": [{"name": "geo_city", "value": None}],
                        "submission_timestamp_granularity": "seconds",
                    },
                    "user-characteristics": {
                        "expiration_policy": {"delete_after_days": 90}
                    },
                },
                "moz_pipeline_metadata_defaults": {
                    "expiration_policy": {"delete_after_days": 400}
                },
                "notification_emails": ["aplacitelli@mozilla.com"],
                "ping_files": ["mobile/android/fenix/app/pings.yaml"],
                "tag_files": ["mobile/android/fenix/app/tags.yaml"],
                "url": "https://github.com/mozilla/gecko-dev",
                "v1_name": "firefox-android-nightly",
            },
        },
        "firefox_ios": {
            "release__0": {
                "app_channel": "release",
                "app_description": "Firefox for iOS",
                "app_id": "org.mozilla.ios.Firefox",
                "app_name": "firefox_ios",
                "bq_dataset_family": "org_mozilla_ios_firefox",
                "branch": "main",
                "canonical_app_name": "Firefox for iOS",
                "dependencies": [
                    "glean-core",
                    "org.mozilla.components:service-glean",
                    "nimbus",
                    "org.mozilla.appservices:logins",
                    "org.mozilla.appservices:syncmanager",
                ],
                "description": "Release channel of Firefox for iOS.",
                "document_namespace": "org-mozilla-ios-firefox",
                "metrics_files": [
                    "firefox-ios/Client/metrics.yaml",
                    "firefox-ios/Storage/metrics.yaml",
                ],
                "moz_pipeline_metadata": {
                    "topsites-impression": {
                        "expiration_policy": {"delete_after_days": 30},
                        "override_attributes": [{"name": "geo_city", "value": None}],
                        "submission_timestamp_granularity": "seconds",
                    }
                },
                "moz_pipeline_metadata_defaults": {
                    "expiration_policy": {"delete_after_days": 775}
                },
                "notification_emails": ["tlong@mozilla.com"],
                "ping_files": ["firefox-ios/Client/pings.yaml"],
                "tag_files": ["firefox-ios/Client/tags.yaml"],
                "url": "https://github.com/mozilla-mobile/firefox-ios",
                "v1_name": "firefox-ios-release",
            },
            "beta__1": {
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
                "document_namespace": "org-mozilla-ios-firefoxbeta",
                "metrics_files": [
                    "firefox-ios/Client/metrics.yaml",
                    "firefox-ios/Storage/metrics.yaml",
                ],
                "moz_pipeline_metadata": {
                    "topsites-impression": {
                        "expiration_policy": {"delete_after_days": 30},
                        "override_attributes": [{"name": "geo_city", "value": None}],
                        "submission_timestamp_granularity": "seconds",
                    }
                },
                "moz_pipeline_metadata_defaults": {
                    "expiration_policy": {"delete_after_days": 775}
                },
                "notification_emails": ["tlong@mozilla.com"],
                "ping_files": ["firefox-ios/Client/pings.yaml"],
                "tag_files": ["firefox-ios/Client/tags.yaml"],
                "url": "https://github.com/mozilla-mobile/firefox-ios",
                "v1_name": "firefox-ios-beta",
            },
            "nightly__2": {
                "app_channel": "nightly",
                "app_description": "Firefox for iOS",
                "app_id": "org.mozilla.ios.Fennec",
                "app_name": "firefox_ios",
                "bq_dataset_family": "org_mozilla_ios_fennec",
                "branch": "main",
                "canonical_app_name": "Firefox for iOS",
                "dependencies": [
                    "glean-core",
                    "org.mozilla.components:service-glean",
                    "nimbus",
                    "org.mozilla.appservices:logins",
                    "org.mozilla.appservices:syncmanager",
                ],
                "description": "Nightly channel of Firefox for iOS.",
                "document_namespace": "org-mozilla-ios-fennec",
                "metrics_files": [
                    "firefox-ios/Client/metrics.yaml",
                    "firefox-ios/Storage/metrics.yaml",
                ],
                "moz_pipeline_metadata": {
                    "topsites-impression": {
                        "expiration_policy": {"delete_after_days": 30},
                        "override_attributes": [{"name": "geo_city", "value": None}],
                        "submission_timestamp_granularity": "seconds",
                    }
                },
                "moz_pipeline_metadata_defaults": {
                    "expiration_policy": {"delete_after_days": 775}
                },
                "notification_emails": ["tlong@mozilla.com"],
                "ping_files": ["firefox-ios/Client/pings.yaml"],
                "tag_files": ["firefox-ios/Client/tags.yaml"],
                "url": "https://github.com/mozilla-mobile/firefox-ios",
                "v1_name": "firefox-ios-dev",
            },
        },
        "focus_ios": {
            "multichannel": {
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
            }
        },
        "focus_android": {
            "release__0": {
                "app_channel": "release",
                "app_description": "Firefox Focus on Android. Klar is the sibling application",
                "app_id": "org.mozilla.focus",
                "app_name": "focus_android",
                "bq_dataset_family": "org_mozilla_focus",
                "canonical_app_name": "Firefox Focus for Android",
                "dependencies": [
                    "glean-core",
                    "org.mozilla.components:service-glean",
                    "org.mozilla.components:lib-crash",
                    "nimbus",
                    "org.mozilla.components:service-nimbus",
                    "gecko",
                ],
                "description": "Release channel of Focus for Android.",
                "document_namespace": "org-mozilla-focus",
                "metrics_files": ["mobile/android/focus-android/app/metrics.yaml"],
                "moz_pipeline_metadata": {
                    "pageload": {"expiration_policy": {"delete_after_days": 400}},
                    "user-characteristics": {
                        "expiration_policy": {"delete_after_days": 90}
                    },
                },
                "moz_pipeline_metadata_defaults": {
                    "expiration_policy": {"delete_after_days": 760}
                },
                "notification_emails": ["jalmeida@mozilla.com", "tlong@mozilla.com"],
                "ping_files": ["mobile/android/focus-android/app/pings.yaml"],
                "url": "https://github.com/mozilla/gecko-dev",
                "v1_name": "firefox-focus-android",
            },
            "beta__1": {
                "app_channel": "beta",
                "app_description": "Firefox Focus on Android. Klar is the sibling application",
                "app_id": "org.mozilla.focus.beta",
                "app_name": "focus_android",
                "bq_dataset_family": "org_mozilla_focus_beta",
                "canonical_app_name": "Firefox Focus for Android",
                "dependencies": [
                    "glean-core",
                    "org.mozilla.components:service-glean",
                    "org.mozilla.components:lib-crash",
                    "nimbus",
                    "org.mozilla.components:service-nimbus",
                    "gecko",
                ],
                "description": "Beta channel of Focus for Android.",
                "document_namespace": "org-mozilla-focus-beta",
                "metrics_files": ["mobile/android/focus-android/app/metrics.yaml"],
                "moz_pipeline_metadata": {
                    "pageload": {"expiration_policy": {"delete_after_days": 400}},
                    "user-characteristics": {
                        "expiration_policy": {"delete_after_days": 90}
                    },
                },
                "moz_pipeline_metadata_defaults": {
                    "expiration_policy": {"delete_after_days": 760}
                },
                "notification_emails": ["jalmeida@mozilla.com", "tlong@mozilla.com"],
                "ping_files": ["mobile/android/focus-android/app/pings.yaml"],
                "url": "https://github.com/mozilla/gecko-dev",
                "v1_name": "firefox-focus-android-beta",
            },
            "nightly__2": {
                "app_channel": "nightly",
                "app_description": "Firefox Focus on Android. Klar is the sibling application",
                "app_id": "org.mozilla.focus.nightly",
                "app_name": "focus_android",
                "bq_dataset_family": "org_mozilla_focus_nightly",
                "canonical_app_name": "Firefox Focus for Android",
                "dependencies": [
                    "glean-core",
                    "org.mozilla.components:service-glean",
                    "org.mozilla.components:lib-crash",
                    "nimbus",
                    "org.mozilla.components:service-nimbus",
                    "gecko",
                ],
                "description": "Nightly channel of Focus for Android.",
                "document_namespace": "org-mozilla-focus-nightly",
                "metrics_files": ["mobile/android/focus-android/app/metrics.yaml"],
                "moz_pipeline_metadata": {
                    "pageload": {"expiration_policy": {"delete_after_days": 400}},
                    "user-characteristics": {
                        "expiration_policy": {"delete_after_days": 90}
                    },
                },
                "moz_pipeline_metadata_defaults": {
                    "expiration_policy": {"delete_after_days": 760}
                },
                "notification_emails": ["jalmeida@mozilla.com", "tlong@mozilla.com"],
                "ping_files": ["mobile/android/focus-android/app/pings.yaml"],
                "url": "https://github.com/mozilla/gecko-dev",
                "v1_name": "firefox-focus-android-nightly",
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
