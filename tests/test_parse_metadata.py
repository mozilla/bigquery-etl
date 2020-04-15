import pytest
from pathlib import Path

from bigquery_etl.parse_metadata import Metadata

TEST_DIR = Path(__file__).parent


class TestParseMetadata(object):
    def test_is_valid_label(self):
        assert Metadata.is_valid_label("valid_label")
        assert Metadata.is_valid_label("valid-label1")
        assert Metadata.is_valid_label("a" * 63)
        assert Metadata.is_valid_label("låbel")
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
        assert metadata.review_bug() is None
        assert "invalid_value" not in metadata.labels
        assert "invalid.label" not in metadata.labels

    def test_non_existing_file(self):
        metadata_file = TEST_DIR / "nonexisting_dir" / "metadata.yaml"
        with pytest.raises(FileNotFoundError):
            Metadata.from_file(metadata_file)

    def test_of_sql_file(self):
        metadata_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "test"
            / "non_incremental_query_v1"
            / "query.sql"
        )
        metadata = Metadata.of_sql_file(metadata_file)

        assert metadata.friendly_name == "Test table for a non-incremental query"
        assert metadata.description == "Test table for a non-incremental query"
        assert metadata.review_bug() == "1999999"

    def test_of_sql_file_no_metadata(self):
        metadata_file = (
            TEST_DIR
            / "data"
            / "test_sql"
            / "test"
            / "no_metadata_query_v1"
            / "query.sql"
        )
        with pytest.raises(FileNotFoundError):
            Metadata.of_sql_file(metadata_file)

    def test_of_table(self):
        metadata = Metadata.of_table(
            "test", "non_incremental_query", "v1", TEST_DIR / "data" / "test_sql"
        )

        assert metadata.friendly_name == "Test table for a non-incremental query"
        assert metadata.description == "Test table for a non-incremental query"
        assert metadata.review_bug() == "1999999"

    def test_of_non_existing_table(self):
        with pytest.raises(FileNotFoundError):
            Metadata.of_table(
                "test", "no_metadata", "v1", TEST_DIR / "data" / "test_sql"
            )
