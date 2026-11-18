"""Tests for excluding configured metrics from the events_stream `metrics` column."""

from pathlib import Path

import pytest

from bigquery_etl.util.common import render
from sql_generators.glean_usage import common as glean_usage_common
from sql_generators.glean_usage.events_stream import get_exclude_metrics

TEMPLATES_DIR = Path(glean_usage_common.__file__).parent / "templates"


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
    with pytest.raises(Exception, match="metrics_as_struct"):
        get_exclude_metrics("firefox_desktop", metrics_as_struct=True)


def _render_events_stream_query(exclude_metrics):
    return render(
        "events_stream_v1.query.sql",
        template_folder=TEMPLATES_DIR,
        header="-- Generated via bigquery_etl.glean_usage\n",
        project_id="moz-fx-data-shared-prod",
        events_view="test_app.events",
        app_name="test_app",
        derived_dataset="test_app_derived",
        has_profile_group_id=False,
        has_legacy_telemetry_client_id=False,
        metrics_as_struct=False,
        has_metrics=True,
        slice_by_sample_id=False,
        exclude_metrics=exclude_metrics,
    )


def test_render_appends_exclude_metrics_to_json_remove():
    rendered = _render_events_stream_query(["$.a.b", "$.c.d"])
    assert (
        "'$.url2',\n      '$.a.b',\n      '$.c.d'\n    ),\n    remove_empty => TRUE"
        in rendered
    )


def test_render_without_exclude_metrics_is_unchanged():
    rendered = _render_events_stream_query([])
    assert "'$.url2'\n    ),\n    remove_empty => TRUE" in rendered
    assert "$.a.b" not in rendered
