from bigquery_etl.util.common import project_dirs


class TestUtilCommon:
    def test_project_dirs(self):
        assert project_dirs("test") == ["test"]

        existing_projects = project_dirs()
        assert "moz-fx-data-shared-prod" in existing_projects
        assert "mozfun" not in existing_projects
