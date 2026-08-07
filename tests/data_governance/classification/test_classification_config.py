import pytest

from bigquery_etl.data_governance.classification.config import ClassificationConfig


class TestClassificationConfig:
    def test_default_table_references(self):
        config = ClassificationConfig(run_id="run-1")

        assert (
            config.profiles_table
            == "moz-fx-data-shared-prod.data_governance_metadata_derived.classification_column_profiles_v1"
        )
        assert (
            config.lineage_table
            == "moz-fx-data-shared-prod.data_governance_metadata_derived.classification_lineage_mapping_v1"
        )
        assert (
            config.probes_table
            == "moz-fx-data-shared-prod.data_governance_metadata_derived.classification_probe_definitions_v1"
        )
        assert (
            config.destination_table
            == "moz-fx-data-shared-prod.data_governance_metadata_derived.column_classifications_v1"
        )

    def test_project_and_dataset_override_repoints_all_tables(self):
        config = ClassificationConfig(
            run_id="run-1", project="moz-fx-data-proto", dataset="akomar"
        )

        assert all(
            table.startswith("moz-fx-data-proto.akomar.")
            for table in (
                config.profiles_table,
                config.lineage_table,
                config.probes_table,
                config.destination_table,
            )
        )

    def test_sanitization_defaults(self):
        config = ClassificationConfig(run_id="run-1")

        assert config.sanitize is True
        assert config.dlp_project is None
        assert config.dlp_quota_project is None

    def test_service_projects_follow_project_by_default(self):
        config = ClassificationConfig(run_id="run-1")

        assert config.vertex_project is None
        assert config.dlp_project is None

    def test_non_gemini_model_rejected(self):
        with pytest.raises(ValueError):
            ClassificationConfig(run_id="run-1", model="claude-sonnet-4-6")

    @pytest.mark.parametrize("run_id", ["", "   "])
    def test_empty_run_id_rejected(self, run_id):
        with pytest.raises(ValueError):
            ClassificationConfig(run_id=run_id)

    @pytest.mark.parametrize(
        "overrides",
        [
            {"project": "moz-fx-data-proto`"},
            {"dataset": "akomar dataset"},
            {"dataset": ""},
        ],
        ids=["backtick_in_project", "space_in_dataset", "empty_dataset"],
    )
    def test_unquotable_table_path_rejected(self, overrides):
        # Both reach a backtick-quoted table reference as text, so they are
        # checked here rather than at each read that interpolates them.
        with pytest.raises(ValueError, match="Invalid BigQuery identifier"):
            ClassificationConfig(run_id="run-1", **overrides)
