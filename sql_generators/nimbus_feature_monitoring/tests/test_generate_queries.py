import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

BASE_DIR = Path(os.path.dirname(__file__)).parent


def make_source_table_spec(name, table_name, source_type="metrics", dimensions=None):
    spec = MagicMock()
    spec.table_name = table_name
    spec.type = source_type
    spec.analysis_unit_id = "client_info.client_id"
    spec.dimensions = dimensions or {}
    return spec


def make_feature_spec(name, slug=None, metrics_by_source=None, ratios=None):
    spec = MagicMock()
    spec.name = name
    spec.ratios = ratios or []
    spec.metrics_by_source = metrics_by_source or {}
    spec.nimbus_slug.return_value = slug or name
    return spec


def make_app_config(dataset, data_sources, features):
    spec = MagicMock()
    spec.dataset = dataset
    spec.data_sources = data_sources
    spec.features = features
    app = MagicMock()
    app.slug = dataset
    app.spec = spec
    return app


class TestGenerateQueries:
    def _run_generate(self, app_configs, tmp_path):
        from sql_generators.nimbus_feature_monitoring import generate_queries

        collection = MagicMock()
        collection.featmon_configs = app_configs
        with patch(
            "sql_generators.nimbus_feature_monitoring.ConfigCollection.from_github_repo",
            return_value=collection,
        ):
            generate_queries("moz-fx-data-shared-prod", BASE_DIR / "templates", tmp_path)

    def test_generates_query_for_metrics_source(self, tmp_path):
        app = make_app_config(
            dataset="firefox_desktop",
            data_sources={
                "metrics": make_source_table_spec("metrics", "metrics", "metrics"),
            },
            features={
                "my_feature": make_feature_spec(
                    "my_feature",
                    slug="my-feature",
                    metrics_by_source={
                        "metrics": {"boolean": {"pref_enabled": None}}
                    },
                )
            },
        )
        self._run_generate([app], tmp_path)
        query = (
            tmp_path
            / "moz-fx-data-shared-prod"
            / "firefox_desktop_derived"
            / "nimbus_feature_monitoring_my_feature_v1"
            / "query.sql"
        )
        assert query.exists()
        sql = query.read_text()
        assert "my-feature" in sql
        assert "pref_enabled" in sql

    def test_generates_query_for_events_stream_source(self, tmp_path):
        app = make_app_config(
            dataset="firefox_desktop",
            data_sources={
                "events_stream": make_source_table_spec(
                    "events_stream", "events_stream", "events_stream"
                ),
            },
            features={
                "my_feature": make_feature_spec(
                    "my_feature",
                    metrics_by_source={
                        "events_stream": {
                            "event": {"my_category": {"my_event": None}}
                        }
                    },
                )
            },
        )
        self._run_generate([app], tmp_path)
        query = (
            tmp_path
            / "moz-fx-data-shared-prod"
            / "firefox_desktop_derived"
            / "nimbus_feature_monitoring_my_feature_v1"
            / "query.sql"
        )
        assert query.exists()
        sql = query.read_text()
        assert "events_stream" in sql
        assert "my_category_my_event" in sql

    def test_generates_view_over_all_features(self, tmp_path):
        features = {
            f"feature_{i}": make_feature_spec(f"feature_{i}") for i in range(3)
        }
        data_sources = {
            "metrics": make_source_table_spec("metrics", "metrics", "metrics"),
        }
        for feat in features.values():
            feat.metrics_by_source = {"metrics": {"boolean": {"flag": None}}}

        app = make_app_config("firefox_desktop", data_sources, features)
        self._run_generate([app], tmp_path)

        view = (
            tmp_path
            / "moz-fx-data-shared-prod"
            / "firefox_desktop"
            / "nimbus_feature_monitoring"
            / "view.sql"
        )
        assert view.exists()
        sql = view.read_text()
        for i in range(3):
            assert f"feature_{i}" in sql

    def test_ratios_populated_in_query(self, tmp_path):
        app = make_app_config(
            dataset="firefox_desktop",
            data_sources={
                "metrics": make_source_table_spec("metrics", "metrics", "metrics"),
            },
            features={
                "my_feature": make_feature_spec(
                    "my_feature",
                    metrics_by_source={
                        "metrics": {"quantity": {"numerator": None, "denominator": None}}
                    },
                    ratios=[["numerator_avg", "denominator_avg"]],
                )
            },
        )
        self._run_generate([app], tmp_path)
        query = (
            tmp_path
            / "moz-fx-data-shared-prod"
            / "firefox_desktop_derived"
            / "nimbus_feature_monitoring_my_feature_v1"
            / "query.sql"
        )
        sql = query.read_text()
        assert "SAFE_DIVIDE" in sql
        assert "numerator_avg" in sql
