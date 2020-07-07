from pathlib import Path

from bigquery_etl.docs.validate_docs import sql_for_dry_run

TEST_DIR = Path(__file__).parent.parent


class TestValidateDocs:
    def test_sql_for_dry_run_no_dependencies(self):
        file = (
            TEST_DIR
            / "data"
            / "test_docs"
            / "generated_docs"
            / "test_dataset1"
            / "examples"
            / "example1.sql"
        )

        sql = sql_for_dry_run(file, {}, "")
        assert sql == open(file).read()
