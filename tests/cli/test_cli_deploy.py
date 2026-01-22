from bigquery_etl.cli.deploy import _build_dependency_graph, _discover_artifacts


class TestArtifactDiscovery:
    def test_discover_tables(self, tmp_path):
        table_dir = tmp_path / "sql/test-project/test_dataset/test_table_v1"
        table_dir.mkdir(parents=True)
        (table_dir / "query.sql").write_text("SELECT 1")

        artifacts = _discover_artifacts(
            paths=(str(table_dir),),
            sql_dir=str(tmp_path / "sql"),
            project_ids=["test-project"],
            artifact_types=["table"],
        )

        assert len(artifacts) == 1
        assert "test-project.test_dataset.test_table_v1" in artifacts
        assert artifacts["test-project.test_dataset.test_table_v1"][1] == "table"

    def test_discover_views(self, tmp_path):
        view_dir = tmp_path / "sql/test-project/test_dataset/test_view"
        view_dir.mkdir(parents=True)
        (view_dir / "view.sql").write_text(
            "CREATE OR REPLACE VIEW `test-project.test_dataset.test_view` AS SELECT 1"
        )

        artifacts = _discover_artifacts(
            paths=(str(view_dir),),
            sql_dir=str(tmp_path / "sql"),
            project_ids=["test-project"],
            artifact_types=["view"],
        )

        assert len(artifacts) == 1
        assert "test-project.test_dataset.test_view" in artifacts
        assert artifacts["test-project.test_dataset.test_view"][1] == "view"

    def test_discover_mixed_artifacts(self, tmp_path):
        table_dir = tmp_path / "sql/test-project/test_dataset/test_table_v1"
        table_dir.mkdir(parents=True)
        (table_dir / "query.sql").write_text("SELECT 1")

        view_dir = tmp_path / "sql/test-project/test_dataset/test_view"
        view_dir.mkdir(parents=True)
        (view_dir / "view.sql").write_text(
            "CREATE OR REPLACE VIEW `test-project.test_dataset.test_view` AS SELECT 1"
        )

        artifacts = _discover_artifacts(
            paths=(str(tmp_path / "sql/test-project"),),
            sql_dir=str(tmp_path / "sql"),
            project_ids=["test-project"],
            artifact_types=["table", "view"],
        )

        assert len(artifacts) == 2
        assert "test-project.test_dataset.test_table_v1" in artifacts
        assert "test-project.test_dataset.test_view" in artifacts


class TestDependencyGraph:
    def test_view_depends_on_table(self, tmp_path):
        table_dir = tmp_path / "sql/test-project/test_dataset/test_table_v1"
        table_dir.mkdir(parents=True)
        (table_dir / "query.sql").write_text("SELECT 1 as value")
        (table_dir / "schema.yaml").write_text(
            "fields:\n- name: value\n  type: INTEGER"
        )

        view_dir = tmp_path / "sql/test-project/test_dataset/test_view"
        view_dir.mkdir(parents=True)
        (view_dir / "view.sql").write_text(
            "CREATE OR REPLACE VIEW `test-project.test_dataset.test_view` AS\n"
            "SELECT * FROM `test-project.test_dataset.test_table_v1`"
        )

        artifacts = {
            "test-project.test_dataset.test_table_v1": (
                table_dir / "query.sql",
                "table",
            ),
            "test-project.test_dataset.test_view": (view_dir / "view.sql", "view"),
        }

        graph = _build_dependency_graph(artifacts)

        assert "test-project.test_dataset.test_table_v1" in graph
        assert "test-project.test_dataset.test_view" in graph
        assert (
            "test-project.test_dataset.test_table_v1"
            in graph["test-project.test_dataset.test_view"]
        )
        assert len(graph["test-project.test_dataset.test_table_v1"]) == 0

    def test_query_without_schema_depends_on_view(self, tmp_path):
        # Test that query without schema.yaml depends on view, which depends on table.
        base_table_dir = tmp_path / "sql/test-project/test_dataset/base_table_v1"
        base_table_dir.mkdir(parents=True)
        (base_table_dir / "query.sql").write_text("SELECT 1 as id, 'test' as name")
        (base_table_dir / "schema.yaml").write_text(
            "fields:\n"
            "- name: id\n"
            "  type: INTEGER\n"
            "- name: name\n"
            "  type: STRING"
        )

        view_dir = tmp_path / "sql/test-project/test_dataset/transform_view"
        view_dir.mkdir(parents=True)
        (view_dir / "view.sql").write_text(
            "CREATE OR REPLACE VIEW `test-project.test_dataset.transform_view` AS\n"
            "SELECT id, UPPER(name) as name FROM `test-project.test_dataset.base_table_v1`"
        )

        external_table_dir = tmp_path / "sql/test-project/test_dataset/external_table"
        external_table_dir.mkdir(parents=True)
        (external_table_dir / "query.sql").write_text(
            "SELECT * FROM `test-project.test_dataset.transform_view`"
        )

        artifacts = {
            "test-project.test_dataset.base_table_v1": (
                base_table_dir / "query.sql",
                "table",
            ),
            "test-project.test_dataset.transform_view": (
                view_dir / "view.sql",
                "view",
            ),
            "test-project.test_dataset.external_table": (
                external_table_dir / "query.sql",
                "table",
            ),
        }

        graph = _build_dependency_graph(artifacts)

        assert len(graph["test-project.test_dataset.base_table_v1"]) == 0

        assert (
            "test-project.test_dataset.base_table_v1"
            in graph["test-project.test_dataset.transform_view"]
        )

        assert (
            "test-project.test_dataset.transform_view"
            in graph["test-project.test_dataset.external_table"]
        )

    def test_table_with_schema_ignores_circular_dependency(self, tmp_path):
        table_dir = tmp_path / "sql/test-project/test_dataset/incremental_table_v1"
        table_dir.mkdir(parents=True)
        (table_dir / "query.sql").write_text(
            "SELECT * FROM `test-project.test_dataset.incremental_table_v1`\n"
            "UNION ALL\n"
            "SELECT * FROM new_data"
        )
        (table_dir / "schema.yaml").write_text("fields:\n- name: id\n  type: INTEGER")

        artifacts = {
            "test-project.test_dataset.incremental_table_v1": (
                table_dir / "query.sql",
                "table",
            ),
        }

        graph = _build_dependency_graph(artifacts)

        assert len(graph["test-project.test_dataset.incremental_table_v1"]) == 0

    def test_view_chain_dependencies(self, tmp_path):

        table_dir = tmp_path / "sql/test-project/test_dataset/base_table_v1"
        table_dir.mkdir(parents=True)
        (table_dir / "query.sql").write_text("SELECT 1 as id")
        (table_dir / "schema.yaml").write_text("fields:\n- name: id\n  type: INTEGER")

        view1_dir = tmp_path / "sql/test-project/test_dataset/view_layer1"
        view1_dir.mkdir(parents=True)
        (view1_dir / "view.sql").write_text(
            "CREATE OR REPLACE VIEW `test-project.test_dataset.view_layer1` AS\n"
            "SELECT * FROM `test-project.test_dataset.base_table_v1`"
        )

        view2_dir = tmp_path / "sql/test-project/test_dataset/view_layer2"
        view2_dir.mkdir(parents=True)
        (view2_dir / "view.sql").write_text(
            "CREATE OR REPLACE VIEW `test-project.test_dataset.view_layer2` AS\n"
            "SELECT * FROM `test-project.test_dataset.view_layer1`"
        )

        artifacts = {
            "test-project.test_dataset.base_table_v1": (
                table_dir / "query.sql",
                "table",
            ),
            "test-project.test_dataset.view_layer1": (view1_dir / "view.sql", "view"),
            "test-project.test_dataset.view_layer2": (view2_dir / "view.sql", "view"),
        }

        graph = _build_dependency_graph(artifacts)

        assert len(graph["test-project.test_dataset.base_table_v1"]) == 0
        assert (
            "test-project.test_dataset.base_table_v1"
            in graph["test-project.test_dataset.view_layer1"]
        )
        assert (
            "test-project.test_dataset.view_layer1"
            in graph["test-project.test_dataset.view_layer2"]
        )
