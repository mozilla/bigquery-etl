from pathlib import Path

import pytest

from bigquery_etl.metadata.parse_metadata import (
    DatasetMetadata,
    ExternalDataSubscriptionMetadata,
    ExternalSharingMetadata,
    Metadata,
    PartitionType,
)

TEST_DIR = Path(__file__).parent.parent


class TestParseMetadata(object):
    def test_metadata_instantiation(self):
        metadata = Metadata(
            "Test metadata", "test description", ["test@example.org"], {}
        )

        assert metadata.friendly_name == "Test metadata"
        assert metadata.description == "test description"
        assert metadata.owners == ["test@example.org"]
        assert metadata.labels == {}
        assert metadata.scheduling == {}

    def test_invalid_owners(self):
        with pytest.raises(ValueError):
            Metadata("Test metadata", "test description", ["testexample.org"])

    def test_valid_external_sharing_subscribers(self):
        # both group: emails and workgroup: identities are accepted
        subscribers = [
            "group:partner@example.org",
            "group:other@example.org",
            "workgroup:mozilla-confidential/data-viewers",
        ]
        metadata = ExternalSharingMetadata(
            exchange="test_exchange",
            data_review="https://bugzilla.mozilla.org/1",
            subscribers=subscribers,
        )

        assert metadata.subscribers == subscribers

    def test_invalid_external_sharing_subscribers(self):
        # bare emails, group: without an email, and workgroups that aren't
        # `<namespace>/<group>` are all rejected
        for bad in (
            ["partner@example.org"],
            ["group:partner-data"],
            ["workgroup:"],
            ["workgroup:some-managed-workgroup"],  # missing /<group>
            ["workgroup:not a workgroup"],
        ):
            with pytest.raises(ValueError):
                ExternalSharingMetadata(
                    exchange="test_exchange",
                    data_review="https://bugzilla.mozilla.org/1",
                    subscribers=bad,
                )

    def test_invalid_external_sharing_resource_id(self):
        for field in ("exchange_id", "listing_id"):
            with pytest.raises(ValueError):
                ExternalSharingMetadata(
                    exchange="test_exchange",
                    data_review="https://bugzilla.mozilla.org/1",
                    subscribers=["group:partner@example.org"],
                    **{field: "has-hyphen"},
                )

    def test_external_sharing_rejects_unknown_key(self, tmp_path):
        # an unknown key (e.g. a typo) must be a hard error, not silently dropped
        (tmp_path / "dataset_metadata.yaml").write_text(
            "friendly_name: T\n"
            "description: T\n"
            "dataset_base_acl: view\n"
            "user_facing: true\n"
            "external_sharing:\n"
            "  exchange: X\n"
            "  data_review: r\n"
            "  restrict_exports: true\n"  # typo of restrict_export
            "  subscribers:\n"
            "    - group:partner@example.org\n"
        )
        with pytest.raises(ValueError):
            DatasetMetadata.from_file(tmp_path / "dataset_metadata.yaml")

    def test_external_sharing_all_fields_round_trip(self, tmp_path):
        # Guards against schema drift from the cloudops-infra sharing module:
        # every key the module reads must map to a field here (unknown keys are
        # now rejected, so a field missing here would break a valid config).
        (tmp_path / "dataset_metadata.yaml").write_text(
            "friendly_name: T\n"
            "description: T\n"
            "dataset_base_acl: view\n"
            "user_facing: true\n"
            "external_sharing:\n"
            "  exchange: Partner Exchange\n"
            "  exchange_id: partner_exchange\n"
            "  exchange_display_name: Mozilla - Partner\n"
            "  exchange_description: Data for partner\n"
            "  listing_id: partner_listing\n"
            "  display_name: Mozilla - Partner Data\n"
            "  listing_description: Custom listing description\n"
            "  restrict_export: true\n"
            "  data_review: https://bugzilla.mozilla.org/1\n"
            "  subscribers:\n"
            "    - group:partner@example.org\n"
        )

        sharing = DatasetMetadata.from_file(
            tmp_path / "dataset_metadata.yaml"
        ).external_sharing

        assert sharing == ExternalSharingMetadata(
            exchange="Partner Exchange",
            data_review="https://bugzilla.mozilla.org/1",
            subscribers=["group:partner@example.org"],
            display_name="Mozilla - Partner Data",
            listing_description="Custom listing description",
            listing_id="partner_listing",
            exchange_id="partner_exchange",
            exchange_display_name="Mozilla - Partner",
            exchange_description="Data for partner",
            restrict_export=True,
        )

    def test_invalid_external_data_subscription_resource_id(self):
        for field in ("data_exchange_id", "listing_id"):
            kwargs = {
                "source_project": "65960090760",
                "data_exchange_id": "partner_exchange",
                "listing_id": "partner_listing",
                field: "has-hyphen",
            }
            with pytest.raises(ValueError):
                ExternalDataSubscriptionMetadata(**kwargs)

    def test_external_data_subscription_rejects_unknown_key(self, tmp_path):
        # an unknown key (e.g. a typo) must be a hard error, not silently dropped
        (tmp_path / "dataset_metadata.yaml").write_text(
            "friendly_name: T\n"
            "description: T\n"
            "dataset_base_acl: view\n"
            "user_facing: true\n"
            "external_data_subscription:\n"
            "  source_project: '65960090760'\n"
            "  data_exchange_id: partner_exchange\n"
            "  listing_id: partner_listing\n"
            "  listings_id: partner_listing\n"  # typo of listing_id
        )
        with pytest.raises(ValueError):
            DatasetMetadata.from_file(tmp_path / "dataset_metadata.yaml")

    def test_external_data_subscription_all_fields_round_trip(self, tmp_path):
        # Guards against schema drift from the cloudops-infra subscription module:
        # every key the module reads must map to a field here (unknown keys are
        # now rejected, so a field missing here would break a valid config).
        (tmp_path / "dataset_metadata.yaml").write_text(
            "friendly_name: T\n"
            "description: T\n"
            "dataset_base_acl: view\n"
            "user_facing: true\n"
            "external_data_subscription:\n"
            "  source_project: '65960090760'\n"
            "  data_exchange_id: partner_exchange\n"
            "  listing_id: partner_listing\n"
            "  friendly_name: PMG Analytics\n"
            "  description: Data shared with us by PMG.\n"
            "  labels:\n"
            "    domain: marketing\n"
        )

        subscription = DatasetMetadata.from_file(
            tmp_path / "dataset_metadata.yaml"
        ).external_data_subscription

        assert subscription == ExternalDataSubscriptionMetadata(
            source_project="65960090760",
            data_exchange_id="partner_exchange",
            listing_id="partner_listing",
            friendly_name="PMG Analytics",
            description="Data shared with us by PMG.",
            labels={"domain": "marketing"},
        )

    def test_invalid_label(self):
        with pytest.raises(ValueError):
            Metadata(
                "Test metadata",
                "test description",
                ["test@example.org"],
                {"INVALID-KEY": "foo"},
            )
        with pytest.raises(ValueError):
            Metadata(
                "Test metadata",
                "test description",
                ["test@example.org"],
                {"foo": "INVALID-VALUE"},
            )

    def test_invalid_review_bugs(self):
        assert Metadata(
            "Test", "Description", ["test@test.org"], {"review_bugs": [123456]}
        )
        with pytest.raises(ValueError):
            Metadata(
                "Test",
                "Description",
                ["test@example.org"],
                {"review_bugs": 123456},
            )

    def test_is_valid_label(self):
        assert Metadata.is_valid_label("valid_label")
        assert Metadata.is_valid_label("valid-label1")
        assert Metadata.is_valid_label("1231")
        assert Metadata.is_valid_label("1231-21")
        assert Metadata.is_valid_label("a" * 63)
        assert Metadata.is_valid_label("låbel") is False
        assert Metadata.is_valid_label("a" * 64) is False
        assert Metadata.is_valid_label("INVALID") is False
        assert Metadata.is_valid_label("invalid.label") is False
        assert Metadata.is_valid_label("") is False

    def test_from_file(self):
        metadata_file = TEST_DIR / "data" / "metadata.yaml"
        metadata = Metadata.from_file(metadata_file)

        assert metadata.friendly_name == "Test metadata file"
        assert metadata.description == "Please provide a description for the query"
        assert "schedule" in metadata.labels
        assert metadata.labels["schedule"] == "daily"
        assert "public_json" in metadata.labels
        assert metadata.labels["public_json"] == ""
        assert metadata.is_public_json()
        assert metadata.is_incremental()
        assert metadata.is_incremental_export()
        assert metadata.review_bugs() is None
        assert "1232341234" in metadata.labels
        assert "1234_abcd" in metadata.labels
        assert "number_value" in metadata.labels
        assert metadata.labels["number_value"] == "1234234"
        assert "number_string" in metadata.labels
        assert metadata.labels["number_string"] == "1234abcde"
        assert "123-432" in metadata.labels
        assert metadata.owners == [
            "test1@mozilla.com",
            "test2@example.com",
            "mozilla/test_group",
        ]
        assert metadata.labels["owner1"] == "test1"
        assert metadata.labels["owner2"] == "test2"
        assert "query.sql" in metadata.references
        assert metadata.references["query.sql"] == ["project.dataset_derived.table_v1"]

    def test_non_existing_file(self):
        metadata_file = TEST_DIR / "nonexisting_dir" / "metadata.yaml"
        with pytest.raises(FileNotFoundError):
            Metadata.from_file(metadata_file)

    def test_of_query_file(self):
        metadata_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "non_incremental_query_v1"
            / "query.sql"
        )
        metadata = Metadata.of_query_file(metadata_file)

        assert metadata.friendly_name == "Test table for a non-incremental query"
        assert metadata.description == "Test table for a non-incremental query"
        assert metadata.review_bugs() == ["1999999", "12121212"]
        assert metadata.workgroup_access is not None
        assert metadata.workgroup_access[0].role == "roles/bigquery.dataViewer"
        assert metadata.workgroup_access[0].members[0] == "workgroup:dataplatform/test"

    def test_of_query_file_no_metadata(self):
        metadata_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "no_metadata_query_v1"
            / "query.sql"
        )
        with pytest.raises(FileNotFoundError):
            Metadata.of_query_file(metadata_file)

    def test_of_table(self):
        metadata = Metadata.of_table(
            "test",
            "non_incremental_query",
            "v1",
            TEST_DIR / "data" / "test_sql" / "moz-fx-data-test-project",
        )

        assert metadata.friendly_name == "Test table for a non-incremental query"
        assert metadata.description == "Test table for a non-incremental query"
        assert metadata.review_bugs() == ["1999999", "12121212"]

    def test_of_non_existing_table(self):
        with pytest.raises(FileNotFoundError):
            Metadata.of_table(
                "test",
                "no_metadata",
                "v1",
                TEST_DIR / "data" / "test_sql" / "moz-fx-data-test-project",
            )

    def test_is_metadata_file(self):
        assert Metadata.is_metadata_file("foo/bar/invalid.json") is False
        assert Metadata.is_metadata_file("foo/bar/invalid.yaml") is False
        assert Metadata.is_metadata_file("metadata.yaml")
        assert Metadata.is_metadata_file("some/path/to/metadata.yaml")

    def test_set_bigquery_partitioning(self):
        metadata = Metadata.of_table(
            "test",
            "non_incremental_query",
            "v1",
            TEST_DIR / "data" / "test_sql" / "moz-fx-data-test-project",
        )
        metadata.set_bigquery_partitioning(
            field="submission_date",
            partition_type=PartitionType.DAY,
            required=True,
            expiration_days=2,
        )

        assert metadata.bigquery.time_partitioning.field == "submission_date"
        assert metadata.bigquery.time_partitioning.type == PartitionType.DAY
        assert metadata.bigquery.time_partitioning.require_partition_filter
        assert metadata.bigquery.time_partitioning.expiration_days == 2
        assert metadata.bigquery.time_partitioning.expiration_ms == 2 * 86400000

    def test_of_deprecated_metadata(self):
        metadata = Metadata.of_table(
            "test",
            "non_incremental_query",
            "v1",
            TEST_DIR / "data" / "test_sql" / "moz-fx-data-test-project",
        )

        assert metadata.deprecated

    def test_of_dataset_metadata(self):
        metadata = DatasetMetadata.from_file(
            TEST_DIR
            / "data"
            / "test_sql"
            / "moz-fx-data-test-project"
            / "test"
            / "dataset_metadata.yaml",
        )

        assert metadata.default_table_workgroup_access[0]["members"] == [
            "test_default_member"
        ]
        assert (
            metadata.default_table_workgroup_access[0]["role"]
            == "roles/bigquery.dataViewer"
        )
