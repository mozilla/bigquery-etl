import pytest

from sql_generators.stable_views_redacted import (
    SUFFIXED_METRIC_TYPES,
    _build_metrics_replacement,
    _get_metrics_struct_fields,
    _get_sensitive_metrics,
    _resolve_bq_type_name,
)

GLEAN_PING_1 = "moz://mozilla.org/schemas/glean/ping/1"
GLEAN_MIN_PING_1 = "moz://mozilla.org/schemas/glean-min/ping/1"


def _make_metric(
    metric_type="text", pings=("microsurvey",), sensitivity=("highly_sensitive",)
):
    """Build a probeinfo-style metric dict for testing."""
    return {
        "type": metric_type,
        "history": [
            {"send_in_pings": list(pings), "data_sensitivity": list(sensitivity)}
        ],
    }


class TestGetSensitiveMetrics:
    """Tests for _get_sensitive_metrics."""

    def test_returns_sensitive_metrics_for_ping(self):
        all_metrics = {
            "microsurvey.event_input_value": _make_metric(
                sensitivity=("stored_content", "highly_sensitive")
            ),
            "microsurvey.event_context": _make_metric(sensitivity=("stored_content",)),
        }
        result = _get_sensitive_metrics(all_metrics, "microsurvey")
        assert len(result) == 1
        assert result[0]["metric_key"] == "microsurvey.event_input_value"
        assert result[0]["bq_column_name"] == "microsurvey_event_input_value"
        assert result[0]["metric_type"] == "text"

    def test_ignores_metrics_not_in_ping(self):
        all_metrics = {
            "microsurvey.event_input_value": _make_metric(pings=("messaging-system",))
        }
        result = _get_sensitive_metrics(all_metrics, "microsurvey")
        assert result == []

    def test_uses_latest_history_entry(self):
        all_metrics = {
            "microsurvey.event_input_value": {
                "type": "text",
                "history": [
                    {
                        "send_in_pings": ["microsurvey"],
                        "data_sensitivity": ["stored_content"],
                    },
                    {
                        "send_in_pings": ["microsurvey"],
                        "data_sensitivity": ["stored_content", "highly_sensitive"],
                    },
                ],
            },
        }
        result = _get_sensitive_metrics(all_metrics, "microsurvey")
        assert len(result) == 1
        assert result[0]["metric_key"] == "microsurvey.event_input_value"

    def test_older_history_not_used(self):
        """Only the latest history entry matters; older sensitive entry is ignored."""
        all_metrics = {
            "microsurvey.event_input_value": {
                "type": "text",
                "history": [
                    {
                        "send_in_pings": ["microsurvey"],
                        "data_sensitivity": ["highly_sensitive"],
                    },
                    {
                        "send_in_pings": ["microsurvey"],
                        "data_sensitivity": ["stored_content"],
                    },
                ],
            },
        }
        result = _get_sensitive_metrics(all_metrics, "microsurvey")
        assert result == []


class TestGetMetricsStructFields:
    """Tests for _get_metrics_struct_fields."""

    def test_extracts_struct_fields(self):
        schema_fields = [
            {"name": "submission_timestamp", "fields": []},
            {
                "name": "metrics",
                "fields": [
                    {
                        "name": "text2",
                        "fields": [
                            {"name": "microsurvey_event_input_value"},
                            {"name": "microsurvey_event_context"},
                        ],
                    },
                    {
                        "name": "quantity",
                        "fields": [
                            {"name": "microsurvey_event_screen_index"},
                        ],
                    },
                ],
            },
        ]
        result = _get_metrics_struct_fields(schema_fields)
        assert result == {
            "text2": ["microsurvey_event_input_value", "microsurvey_event_context"],
            "quantity": ["microsurvey_event_screen_index"],
        }

    def test_no_metrics_field_returns_empty(self):
        assert (
            _get_metrics_struct_fields([{"name": "submission_timestamp", "fields": []}])
            == {}
        )
        assert _get_metrics_struct_fields([{"name": "metrics", "fields": []}]) == {}


class TestResolveBqTypeName:
    """Tests for _resolve_bq_type_name."""

    def test_direct_match(self):
        struct_types = {
            "string": ["microsurvey_event"],
            "quantity": ["microsurvey_event_screen_index"],
        }
        assert (
            _resolve_bq_type_name("string", GLEAN_MIN_PING_1, struct_types) == "string"
        )
        assert (
            _resolve_bq_type_name("quantity", GLEAN_MIN_PING_1, struct_types)
            == "quantity"
        )

    def test_glean_min_uses_suffixed(self):
        struct_types = {"text2": ["microsurvey_event_input_value"]}
        assert _resolve_bq_type_name("text", GLEAN_MIN_PING_1, struct_types) == "text2"

    def test_glean_ping_1_uses_unsuffixed(self):
        struct_types = {"text": ["microsurvey_event_input_value"]}
        assert _resolve_bq_type_name("text", GLEAN_PING_1, struct_types) == "text"

    def test_all_suffixed_types_glean_min(self):
        for probeinfo_type, suffixed in SUFFIXED_METRIC_TYPES.items():
            struct_types = {suffixed: ["some_column"]}
            assert (
                _resolve_bq_type_name(probeinfo_type, GLEAN_MIN_PING_1, struct_types)
                == suffixed
            )

    def test_unknown_type_returns_none(self):
        assert _resolve_bq_type_name("nonexistent", GLEAN_MIN_PING_1, {}) is None


class TestBuildMetricsReplacement:
    """Tests for _build_metrics_replacement."""

    def test_partial_exclusion(self):
        """One sensitive text metric among several text metrics."""
        sensitive = [
            {
                "metric_key": "microsurvey.event_input_value",
                "bq_column_name": "microsurvey_event_input_value",
                "metric_type": "text",
            }
        ]
        struct_types = {
            "text2": [
                "microsurvey_event_input_value",
                "microsurvey_event_context",
                "microsurvey_message_id",
            ]
        }
        result = _build_metrics_replacement(sensitive, struct_types, GLEAN_MIN_PING_1)
        assert result == (
            "(SELECT AS STRUCT metrics.* REPLACE("
            "(SELECT AS STRUCT metrics.text2.* EXCEPT(microsurvey_event_input_value)) AS text2"
            ")) AS metrics"
        )

    def test_full_exclusion(self):
        """All text metrics are sensitive — exclude the entire text2 struct."""
        sensitive = [
            {
                "metric_key": "microsurvey.event_input_value",
                "bq_column_name": "microsurvey_event_input_value",
                "metric_type": "text",
            },
            {
                "metric_key": "microsurvey.event_context",
                "bq_column_name": "microsurvey_event_context",
                "metric_type": "text",
            },
        ]
        struct_types = {
            "text2": ["microsurvey_event_input_value", "microsurvey_event_context"]
        }
        result = _build_metrics_replacement(sensitive, struct_types, GLEAN_MIN_PING_1)
        assert result == "(SELECT AS STRUCT metrics.* EXCEPT(text2)) AS metrics"

    def test_mixed_exclusion(self):
        """Sensitive metrics across types: partial text exclusion + full quantity exclusion."""
        sensitive = [
            {
                "metric_key": "microsurvey.event_input_value",
                "bq_column_name": "microsurvey_event_input_value",
                "metric_type": "text",
            },
            {
                "metric_key": "microsurvey.event_screen_index",
                "bq_column_name": "microsurvey_event_screen_index",
                "metric_type": "quantity",
            },
        ]
        struct_types = {
            "text2": ["microsurvey_event_input_value", "microsurvey_event_context"],
            "quantity": ["microsurvey_event_screen_index"],
        }
        result = _build_metrics_replacement(sensitive, struct_types, GLEAN_MIN_PING_1)
        assert result == (
            "(SELECT AS STRUCT metrics.* EXCEPT(quantity) REPLACE("
            "(SELECT AS STRUCT metrics.text2.* EXCEPT(microsurvey_event_input_value)) AS text2"
            ")) AS metrics"
        )

    def test_unknown_type_raises(self):
        sensitive = [
            {
                "metric_key": "microsurvey.event_input_value",
                "bq_column_name": "microsurvey_event_input_value",
                "metric_type": "unknown",
            }
        ]
        struct_types = {"text2": ["microsurvey_event_input_value"]}
        with pytest.raises(ValueError, match="Could not resolve BQ type"):
            _build_metrics_replacement(sensitive, struct_types, GLEAN_MIN_PING_1)

    def test_glean_ping_1_type_resolution(self):
        """For glean/ping/1 schemas, text type resolves to 'text' not 'text2'."""
        sensitive = [
            {
                "metric_key": "microsurvey.event_input_value",
                "bq_column_name": "microsurvey_event_input_value",
                "metric_type": "text",
            }
        ]
        struct_types = {
            "text": ["microsurvey_event_input_value", "microsurvey_event_context"]
        }
        result = _build_metrics_replacement(sensitive, struct_types, GLEAN_PING_1)
        assert "metrics.text.* EXCEPT(microsurvey_event_input_value)" in result
