from pathlib import Path

import pytest

from bigquery_etl.metadata.parse_metadata import Metadata

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
        assert metadata.description is None
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
        assert metadata.owners == ["test1@mozilla.com", "test2@example.com"]

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
