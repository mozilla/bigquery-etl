from datetime import date

from bigquery_etl.metadata.parse_metadata import (
    DatasetMetadata,
    ExternalSharingMetadata,
    Metadata,
)
from bigquery_etl.metadata.validate_metadata import (
    validate_dataset_classification,
    validate_deprecation,
    validate_external_sharing,
    validate_public_data,
)


class TestValidateMetadata(object):
    def test_is_valid_public_data(self):
        metadata_not_public = Metadata("No public data", "No public data", {}, {})
        assert validate_public_data(metadata_not_public, "test/path/metadata.yaml")

        metadata_valid_public = Metadata(
            "Public json data",
            "Public json data",
            [],
            {"public_json": True, "review_bugs": [123456]},
            {},
        )
        assert validate_public_data(metadata_valid_public, "test/path/metadata.yaml")

        metadata_valid_public = Metadata(
            "Public BigQuery data",
            "Public BigQuery data",
            [],
            {"public_bigquery": True, "review_bugs": [123456]},
            {},
        )
        assert validate_public_data(metadata_valid_public, "test/path/metadata.yaml")

        metadata_invalid_public = Metadata(
            "Public BigQuery data",
            "Public BigQuery data",
            [],
            {"public_bigquery": True},
            {},
        )
        assert (
            validate_public_data(metadata_invalid_public, "test/path/metadata.yaml")
            is False
        )

    def test_validate_external_sharing(self):
        external_sharing = ExternalSharingMetadata(
            exchange="test_exchange",
            data_review="https://bugzilla.mozilla.org/1",
            subscribers=["group:partner@example.org"],
        )

        def _dataset_metadata(external_sharing=None):
            return DatasetMetadata(
                friendly_name="test",
                description="test",
                dataset_base_acl="view",
                user_facing=True,
                external_sharing=external_sharing,
            )

        # no external_sharing configured -> always valid
        assert validate_external_sharing("some_dataset", _dataset_metadata())

        # valid: dataset suffixed with _shared
        assert validate_external_sharing(
            "foo_shared", _dataset_metadata(external_sharing)
        )

        # invalid: dataset not suffixed with _shared (e.g. the _shared_derived
        # source dataset is not itself shareable)
        assert (
            validate_external_sharing(
                "foo_shared_derived", _dataset_metadata(external_sharing)
            )
            is False
        )
        assert (
            validate_external_sharing(
                "foo_derived", _dataset_metadata(external_sharing)
            )
            is False
        )

    def test_validate_deprecation(self):
        metadata_valid = Metadata(
            friendly_name="test",
            description="test",
            owners=["test@example.org"],
            labels={"test": "true", "foo": "abc"},
            deprecated=True,
            deletion_date=date(2024, 5, 4),
        )

        assert validate_deprecation(metadata_valid, "test/path/metadata.yaml")

        metadata_valid = Metadata(
            friendly_name="test",
            description="test",
            owners=["test@example.org"],
            labels={"test": "true", "foo": "abc"},
            deprecated=True,
            deletion_date=None,
        )

        assert validate_deprecation(metadata_valid, "test/path/metadata.yaml")

        metadata_valid = Metadata(
            friendly_name="test",
            description="test",
            owners=["test@example.org"],
            labels={"test": "true", "foo": "abc"},
            deprecated=False,
            deletion_date=date(2024, 5, 4),
        )

        assert not validate_deprecation(metadata_valid, "test/path/metadata.yaml")

    def test_validate_dataset_classification_user_facing_valid(self):
        metadata = DatasetMetadata(
            friendly_name="Test",
            description="Test",
            dataset_base_acl="view",
            user_facing=True,
        )
        assert validate_dataset_classification("telemetry", metadata)

    def test_validate_dataset_classification_non_user_facing_valid(self):
        metadata = DatasetMetadata(
            friendly_name="Test",
            description="Test",
            dataset_base_acl="derived",
            user_facing=False,
        )
        assert validate_dataset_classification("telemetry_derived", metadata)

    def test_validate_dataset_classification_live_suffix(self):
        metadata = DatasetMetadata(
            friendly_name="Test",
            description="Test",
            dataset_base_acl="derived",
            user_facing=False,
        )
        assert not validate_dataset_classification("telemetry_live", metadata)

    def test_validate_dataset_classification_stable_suffix(self):
        metadata = DatasetMetadata(
            friendly_name="Test",
            description="Test",
            dataset_base_acl="derived",
            user_facing=False,
        )
        assert not validate_dataset_classification("telemetry_stable", metadata)

    def test_validate_dataset_classification_user_facing_with_internal_suffix(self):
        metadata = DatasetMetadata(
            friendly_name="Test",
            description="Test",
            dataset_base_acl="view",
            user_facing=True,
        )
        assert not validate_dataset_classification("telemetry_derived", metadata)

    def test_validate_dataset_classification_non_user_facing_without_internal_suffix(
        self,
    ):
        metadata = DatasetMetadata(
            friendly_name="Test",
            description="Test",
            dataset_base_acl="derived",
            user_facing=False,
        )
        assert not validate_dataset_classification("telemetry", metadata)
