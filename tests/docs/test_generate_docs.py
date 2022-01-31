import os
from pathlib import Path

import pytest

from bigquery_etl.docs.generate_docs import load_with_examples

TEST_DIR = Path(__file__).parent.parent


class TestGenerateDocs:
    def test_load_with_examples_udf(self):
        input = (
            TEST_DIR
            / "data"
            / "test_docs"
            / "generated_docs"
            / "test_dataset1"
            / "udf1"
            / "README.md"
        )
        result = load_with_examples(str(input)).strip()

        assert result == "```sql\nSELECT\n  *\nFROM\n  test\n```"

    def test_load_with_examples_dataset(self):
        input = (
            TEST_DIR
            / "data"
            / "test_docs"
            / "generated_docs"
            / "test_dataset1"
            / "README.md"
        )
        result = load_with_examples(str(input)).strip()

        assert result == "# test_dataset1\n\n```sql\nSELECT\n  *\nFROM\n  test\n```"

    def test_load_with_missing_example(self, tmp_path):
        file_path = tmp_path / "ds" / "udf"
        os.makedirs(file_path)
        file = file_path / "README.md"
        file.write_text("@sql(examples/non_existing.sql)")

        with pytest.raises(FileNotFoundError):
            load_with_examples(str(input)).strip()
