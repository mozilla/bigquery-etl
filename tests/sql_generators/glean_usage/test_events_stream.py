"""Tests for excluding configured metrics from the events_stream `metrics` column."""

import pytest

from sql_generators.glean_usage.events_stream import get_exclude_metrics


@pytest.fixture(autouse=True)
def configured(monkeypatch):
    monkeypatch.setattr(
        "sql_generators.glean_usage.events_stream.ConfigLoader.get",
        lambda *args, fallback=None: (
            {"firefox_desktop": ["$.boolean.os_environment_is_taskbar_pinned"]}
            if args == ("generate", "glean_usage", "events_stream", "exclude_metrics")
            else fallback
        ),
    )


def test_configured_app_returns_exclude_paths():
    assert get_exclude_metrics("firefox_desktop", metrics_as_struct=False) == [
        "$.boolean.os_environment_is_taskbar_pinned"
    ]


def test_unconfigured_app_returns_empty_list():
    assert get_exclude_metrics("fenix", metrics_as_struct=False) == []


def test_struct_app_with_exclude_metrics_raises():
    with pytest.raises(Exception):
        get_exclude_metrics("firefox_desktop", metrics_as_struct=True)
