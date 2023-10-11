from pathlib import Path

from bigquery_etl.routine.parse_routine import sub_local_routines

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

        sql = sub_local_routines(file.read_text(), project_dir="", raw_routines={})
        assert sql == file.read_text()
