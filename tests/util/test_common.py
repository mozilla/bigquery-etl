from bigquery_etl.util.common import project_dirs


class TestUtilCommon:
    def test_project_dirs(self):
        assert project_dirs("test") == ["sql/test"]

        existing_projects = project_dirs()
        assert "sql/moz-fx-data-shared-prod" in existing_projects
