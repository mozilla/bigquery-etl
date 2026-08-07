import logging
from types import SimpleNamespace

import pytest

from bigquery_etl.data_governance.classification import sanitize as sanitize_module
from bigquery_etl.data_governance.classification.config import ClassificationConfig
from bigquery_etl.data_governance.classification.sanitize import (
    DROP,
    KEEP,
    MASK,
    TIER1_MASK_INFOTYPES,
    TIER2_DROP_INFOTYPES,
    Sanitizer,
    sanitize_columns,
    sanitizer_for,
    suppress_high_cardinality_samples,
)

# Derived, not listed: a copy that fell behind would make the fake report
# configured infoTypes as unsupported, silently skipping what each test names.
SUPPORTED_INFO_TYPES = sorted(TIER1_MASK_INFOTYPES | TIER2_DROP_INFOTYPES)


class FakeDlpClient:
    """Stands in for dlp_v2.DlpServiceClient, driven by a per-value response map."""

    def __init__(self, responses=None, supported=None):
        # {input value: (deidentified value, [(info_type, count), ...])}
        self.responses = responses or {}
        self.supported = SUPPORTED_INFO_TYPES if supported is None else supported
        self.requests = []

    def list_info_types(self):
        return SimpleNamespace(
            info_types=[SimpleNamespace(name=name) for name in self.supported]
        )

    def deidentify_content(self, request):
        self.requests.append(request)
        value = request["item"]["value"]
        deidentified, findings = self.responses.get(value, (value, []))
        summaries = [
            SimpleNamespace(
                info_type=SimpleNamespace(name=name),
                results=[SimpleNamespace(count=count)],
            )
            for name, count in findings
        ]
        return SimpleNamespace(
            item=SimpleNamespace(value=deidentified),
            overview=SimpleNamespace(transformation_summaries=summaries),
        )


@pytest.fixture
def make_client_spy(monkeypatch):
    """Record the quota project sanitizer_for passes to _make_client."""
    seen = {}

    def fake_make_client(quota_project):
        seen["quota_project"] = quota_project
        return FakeDlpClient()

    monkeypatch.setattr(sanitize_module, "_make_client", fake_make_client)
    return seen


def build_sanitizer(responses=None, supported=None):
    client = FakeDlpClient(responses=responses, supported=supported)
    return Sanitizer(project="test-project", client=client), client


class TestSanitizer:
    def test_unsupported_info_type_is_skipped_and_warned(self, caplog):
        supported = [t for t in SUPPORTED_INFO_TYPES if t != "US_DEA_NUMBER"]

        with caplog.at_level(logging.WARNING):
            sanitizer, _ = build_sanitizer(supported=supported)

        assert "US_DEA_NUMBER" in caplog.text
        assert "US_DEA_NUMBER" not in sanitizer.drop
        assert "EMAIL_ADDRESS" in sanitizer.mask
        assert "CREDIT_CARD_NUMBER" in sanitizer.drop

    @pytest.mark.parametrize(
        "value,deidentified,findings,expected",
        [
            (
                "contact jane@example.com",
                "contact [EMAIL_ADDRESS]",
                [("EMAIL_ADDRESS", 1)],
                (MASK, "contact [EMAIL_ADDRESS]", ["EMAIL_ADDRESS"]),
            ),
            (
                "ssn 123-45-6789",
                "ssn [SPECIAL_CATEGORY_REMOVED]",
                [("US_SOCIAL_SECURITY_NUMBER", 1)],
                (DROP, None, ["US_SOCIAL_SECURITY_NUMBER"]),
            ),
            # The sentinel alone is enough, with no transformation summaries at
            # all: the belt to the fired-infoTypes braces.
            (
                "card 4111111111111111",
                "card [SPECIAL_CATEGORY_REMOVED]",
                [],
                (DROP, None, []),
            ),
            ("firefox", "firefox", [], (KEEP, "firefox", [])),
        ],
        ids=["tier1_masks", "tier2_drops", "sentinel_alone_drops", "no_findings_keeps"],
    )
    def test_verdict(self, value, deidentified, findings, expected):
        sanitizer, _ = build_sanitizer({value: (deidentified, findings)})

        result = sanitizer.sanitize(value)

        assert (result.action, result.value, result.info_types) == expected

    @pytest.mark.parametrize("value", [None, "", "   "])
    def test_blank_values_skip_dlp(self, value):
        sanitizer, client = build_sanitizer()

        result = sanitizer.sanitize(value)

        assert result.action == KEEP
        assert result.value == value
        assert client.requests == []

    def test_repeated_value_hits_the_cache(self):
        sanitizer, client = build_sanitizer(
            {"jane@example.com": ("[EMAIL_ADDRESS]", [("EMAIL_ADDRESS", 1)])}
        )

        first = sanitizer.sanitize("jane@example.com")
        second = sanitizer.sanitize("jane@example.com")

        assert first == second
        assert len(client.requests) == 1

    def test_sanitize_many_dedupes_and_keys_by_input(self):
        sanitizer, client = build_sanitizer(
            {"jane@example.com": ("[EMAIL_ADDRESS]", [("EMAIL_ADDRESS", 1)])}
        )

        results = sanitizer.sanitize_many(
            ["jane@example.com", "firefox", "jane@example.com", None]
        )

        assert set(results) == {"jane@example.com", "firefox"}
        assert results["jane@example.com"].value == "[EMAIL_ADDRESS]"
        assert results["firefox"].action == KEEP
        assert len(client.requests) == 2


class TestSanitizeColumns:
    def test_masks_drops_and_reports_changes(self):
        sanitizer, _ = build_sanitizer(
            {
                "jane@example.com": ("[EMAIL_ADDRESS]", [("EMAIL_ADDRESS", 1)]),
                "555-1234 call me": (
                    "[PHONE_NUMBER] call me",
                    [("PHONE_NUMBER", 1)],
                ),
                "123-45-6789": (
                    "[SPECIAL_CATEGORY_REMOVED]",
                    [("US_SOCIAL_SECURITY_NUMBER", 1)],
                ),
            }
        )
        columns = [
            {
                "column_name": "contact",
                "example_value": "jane@example.com",
                "values": [
                    ("555-1234 call me", 7),
                    ("123-45-6789", 5),
                    ("firefox", 3),
                ],
            }
        ]

        changes = sanitize_columns(sanitizer, columns)

        assert columns[0]["example_value"] == "[EMAIL_ADDRESS]"
        assert columns[0]["values"] == [("[PHONE_NUMBER] call me", 7), ("firefox", 3)]
        assert changes == [
            {
                "column": "contact",
                "field": "example_value",
                "action": MASK,
                "info_types": ["EMAIL_ADDRESS"],
            },
            {
                "column": "contact",
                "field": "values",
                "action": MASK,
                "info_types": ["PHONE_NUMBER"],
            },
            {
                "column": "contact",
                "field": "values",
                "action": DROP,
                "info_types": ["US_SOCIAL_SECURITY_NUMBER"],
            },
        ]

    def test_columns_without_samples_make_no_dlp_call(self):
        sanitizer, client = build_sanitizer()
        columns = [
            {"column_name": "a", "example_value": None, "values": []},
            {"column_name": "b"},
        ]

        assert sanitize_columns(sanitizer, columns) == []
        assert client.requests == []


class TestSuppressHighCardinalitySamples:
    def test_clears_high_cardinality_samples_only(self):
        columns = [
            {
                "column_name": "client_id",
                "example_value": "abc123",
                "values": [("abc123", 1)],
                "is_high_cardinality": True,
            },
            {
                "column_name": "channel",
                "example_value": "release",
                "values": [("release", 10)],
                "is_high_cardinality": False,
            },
        ]

        assert suppress_high_cardinality_samples(columns) == 1
        assert columns[0]["example_value"] is None
        assert columns[0]["values"] == []
        assert columns[1]["example_value"] == "release"
        assert columns[1]["values"] == [("release", 10)]


class TestSanitizerFor:
    def test_disabled_returns_none_and_warns(self, caplog, monkeypatch):
        def explode(quota_project):
            raise AssertionError("must not build a DLP client")

        monkeypatch.setattr(sanitize_module, "_make_client", explode)
        config = ClassificationConfig(run_id="run-1", sanitize=False)

        with caplog.at_level(logging.WARNING):
            assert sanitizer_for(config) is None

        assert "Sanitization disabled" in caplog.text

    @pytest.mark.parametrize(
        "overrides,expected_parent,expected_quota",
        [
            # Quota is not defaulted to the DLP project: overriding the
            # credentials' quota project would require
            # serviceusage.serviceUsageConsumer there.
            ({}, "projects/moz-fx-data-proto/locations/global", None),
            (
                {"dlp_project": "dlp-project", "dlp_quota_project": "quota-project"},
                "projects/dlp-project/locations/global",
                "quota-project",
            ),
        ],
    )
    def test_dlp_projects_resolved_from_config(
        self, make_client_spy, overrides, expected_parent, expected_quota
    ):
        config = ClassificationConfig(
            run_id="run-1", project="moz-fx-data-proto", **overrides
        )

        sanitizer = sanitizer_for(config)

        assert sanitizer.parent == expected_parent
        assert make_client_spy["quota_project"] == expected_quota
