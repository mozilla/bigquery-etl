import pytest

from bigquery_etl.util.common import project_dirs, render


class TestUtilCommon:
    def test_project_dirs(self):
        assert project_dirs("test") == ["sql/test"]

        existing_projects = project_dirs()
        assert "sql/moz-fx-data-shared-prod" in existing_projects

    def test_metrics_render(self, tmp_path):
        file_path = tmp_path / "test_query.sql"
        file_path.write_text(
            r"""
            SELECT * FROM (
                {{ metrics.calculate(
                    metrics=['days_of_use'],
                    platform='firefox_desktop'
                ) }}
            )
        """
        )
        rendered_sql = render(file_path.name, template_folder=file_path.parent)
        assert r"{{ metrics.calculate" not in rendered_sql
        assert "days_of_use" in rendered_sql

    def test_non_existing_metrics_render(self, tmp_path):
        file_path = tmp_path / "test_query.sql"
        file_path.write_text(
            r"""
            SELECT * FROM (
                {{ metrics.calculate(
                    metrics=['not-existing'],
                    platform='firefox_desktop'
                ) }}
            )
        """
        )

        with pytest.raises(ValueError):
            render(file_path.name, template_folder=file_path.parent)

    def test_render_multiple_metrics(self, tmp_path):
        file_path = tmp_path / "test_query.sql"
        file_path.write_text(
            r"""
            SELECT * FROM (
                {{ metrics.calculate(
                    metrics=['days_of_use', 'uri_count', 'ad_clicks'],
                    platform='firefox_desktop',
                    group_by={'sample_id': 'sample_id'},
                    where='submission_date = "2023-01-01"'
                ) }}
            )
        """
        )
        rendered_sql = render(file_path.name, template_folder=file_path.parent)
        assert "metrics.calculate" not in rendered_sql
        assert r"{{" not in rendered_sql
        assert "days_of_use" in rendered_sql
        assert "clients_daily" in rendered_sql
        assert "uri_count" in rendered_sql
        assert "ad_clicks" in rendered_sql
        assert "mozdata.search.search_clients_engines_sources_daily" in rendered_sql
        assert 'submission_date = "2023-01-01"' in rendered_sql
        assert "sample_id" in rendered_sql

    def test_render_data_source(self, tmp_path):
        file_path = tmp_path / "test_query.sql"
        file_path.write_text(
            r"""
            SELECT * FROM (
                {{ metrics.data_source(
                    data_source="main",
                    platform='firefox_desktop',
                    where='submission_date = "2023-01-01"'
                ) }}
            )
        """
        )
        rendered_sql = render(file_path.name, template_folder=file_path.parent)
        assert "metrics.data_source" not in rendered_sql
        assert r"{{" not in rendered_sql
        assert "main" in rendered_sql
        assert 'submission_date = "2023-01-01"' in rendered_sql

    def test_checks_render(self, tmp_path):
        file_path = tmp_path / "checks.sql"
        file_path.write_text(
            r"""
            {{ min_row_count(1, "submission_date = @submission_date") }}
        """
        )
        kwargs = {
            "project_id": "project",
            "dataset_id": "dataset",
            "table_name": "table",
        }
        rendered_sql = render(
            file_path.name, template_folder=file_path.parent, **kwargs
        )
        assert (
            r"""{{ min_row_count(1, "submission_date = @submission_date") }}"""
            not in rendered_sql
        )
        assert "SELECT" in rendered_sql
        assert "`project.dataset.table`" in rendered_sql
