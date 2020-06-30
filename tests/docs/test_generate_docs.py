import os
from pathlib import Path
import pytest
import shutil

from bigquery_etl.docs.generate_docs import load_with_examples, generate_docs

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

        assert result == "# udf1\n\n```sql\nSELECT\n  *\nFROM\n  test\n\n```"

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

        assert result == "# test_dataset1\n\n```sql\nSELECT\n  *\nFROM\n  test\n\n```"

    def test_load_with_missing_example(self, tmp_path):
        file_path = tmp_path / "ds" / "udf"
        os.makedirs(file_path)
        file = file_path / "README.md"
        file.write_text("@sql(examples/non_existing.sql)")

        with pytest.raises(FileNotFoundError):
            load_with_examples(str(input)).strip()

    def test_generate_docs(self, tmp_path):
        os.chdir(TEST_DIR / "data" / "test_docs")

        if os.path.exists(tmp_path):
            shutil.rmtree(tmp_path)
        shutil.copytree("generated_docs", tmp_path)

        mkdocs = generate_docs(tmp_path, ["generated_docs"], "docs/mkdocs.yml")

        expected_mkdocs = {
            "repo_url": "https://github.com/mozilla/bigquery-etl/",
            "site_author": "Mozilla Data Platform Team",
            "site_description": "Mozilla BigQuery ETL",
            "site_name": "BigQuery ETL",
            "docs_dir": "test_docs",
            "nav": [
                {"Home": "index.md"},
                {"Static": [{"Page1": "index.md"}]},
                {
                    "generated_docs": [
                        {"Overview": "generated_docs/index.md"},
                        {
                            "test_dataset2": [
                                {"Overview": "generated_docs/test_dataset2/index.md"},
                                {"udf2": "generated_docs/test_dataset2/udf2/index.md"},
                                {"udf1": "generated_docs/test_dataset2/udf1/index.md"},
                            ]
                        },
                        {
                            "test_dataset1": [
                                {"Overview": "generated_docs/test_dataset1/index.md"},
                                {"udf2": "generated_docs/test_dataset1/udf2/index.md"},
                                {"udf1": "generated_docs/test_dataset1/udf1/index.md"},
                            ]
                        },
                    ]
                },
            ],
            "theme": {"name": "mkdocs"},
        }

        assert expected_mkdocs == mkdocs
