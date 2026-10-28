"""Tests for skipping Bigeye monitoring on restricted glean_usage derived datasets."""

import pytest

from sql_generators.glean_usage.common import monitoring_enabled

PROJECT = "moz-fx-data-shared-prod"


@pytest.fixture(autouse=True)
def gated(monkeypatch):
    monkeypatch.setattr(
        "bigquery_etl.sensitivity.load_gated_datasets",
        lambda: {
            PROJECT: {"acme_stable": {"default": ["workgroup:acme/data-viewers"]}}
        },
    )
    monkeypatch.setattr(
        "sql_generators.glean_usage.common.BIGCONFIG_SKIP_APPS", ["skipped"]
    )


def test_ungated_app_is_monitored():
    assert monitoring_enabled(PROJECT, "other", "other_derived", ["other"])


def test_skip_list_disables_monitoring():
    assert not monitoring_enabled(PROJECT, "skipped", "skipped_derived", ["skipped"])


def test_restricted_derived_dataset_is_not_monitored():
    assert not monitoring_enabled(PROJECT, "acme", "acme_derived", ["acme"])


def test_app_with_a_gated_channel_is_not_monitored():
    assert not monitoring_enabled(PROJECT, "mixed", "mixed_derived", ["other", "acme"])
