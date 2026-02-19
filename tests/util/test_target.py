import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from bigquery_etl.util.target import Target, get_target


class TestTarget:
    def test_target_creation(self):
        """Test creating a Target instance."""
        target = Target(
            name="test", project_id="test-project", dataset_prefix="test_prefix_"
        )
        assert target.name == "test"
        assert target.project_id == "test-project"
        assert target.dataset_prefix == "test_prefix_"

    def test_target_optional_prefix(self):
        """Test Target with no dataset_prefix."""
        target = Target(name="test", project_id="test-project")
        assert target.name == "test"
        assert target.project_id == "test-project"
        assert target.dataset_prefix is None

    @patch("bigquery_etl.util.target.ConfigLoader")
    @patch("bigquery_etl.util.target.git.Repo")
    def test_get_target_success(self, mock_repo, mock_config_loader):
        """Test successfully getting a target with git info."""
        # Setup mock git repo
        mock_branch = MagicMock()
        mock_branch.name = "main"
        mock_commit = MagicMock()
        mock_commit.hexsha = "abc123def456"
        mock_branch.commit = mock_commit
        mock_repo.return_value.active_branch = mock_branch

        # Create temp targets file
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            targets_file = tmpdir_path / "targets.yaml"
            targets_file.write_text("""
dev:
  project_id: test-project-dev
  dataset_prefix: dev_{{ git.branch }}_{{ git.commit }}_
prod:
  project_id: test-project-prod
""")

            # Setup mock config loader
            mock_config_loader.get.return_value = "targets.yaml"
            mock_config_loader.project_dir = tmpdir_path

            target = get_target("dev")

            assert target.name == "dev"
            assert target.project_id == "test-project-dev"
            assert target.dataset_prefix == "dev_main_abc123de_"

    @patch("bigquery_etl.util.target.ConfigLoader")
    @patch("bigquery_etl.util.target.git.Repo")
    def test_get_target_no_git_repo(self, mock_repo, mock_config_loader):
        """Test getting a target when not in a git repo."""
        # Setup git to raise exception
        mock_repo.side_effect = Exception("Not a git repository")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            targets_file = tmpdir_path / "targets.yaml"
            targets_file.write_text("""
dev:
  project_id: test-project-dev
  dataset_prefix: dev_{{ git.branch }}_{{ git.commit }}_
""")

            mock_config_loader.get.return_value = "targets.yaml"
            mock_config_loader.project_dir = tmpdir_path

            target = get_target("dev")

            assert target.name == "dev"
            assert target.project_id == "test-project-dev"
            assert target.dataset_prefix == "dev_unknown_unknown_"

    @patch("bigquery_etl.util.target.ConfigLoader")
    def test_get_target_not_found(self, mock_config_loader):
        """Test getting a target that doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            targets_file = tmpdir_path / "targets.yaml"
            targets_file.write_text("""
dev:
  project_id: test-project-dev
""")

            mock_config_loader.get.return_value = "targets.yaml"
            mock_config_loader.project_dir = tmpdir_path

            with pytest.raises(Exception, match="Couldn't find target `nonexistent`"):
                get_target("nonexistent")

    @patch("bigquery_etl.util.target.ConfigLoader")
    def test_get_target_file_not_found(self, mock_config_loader):
        """Test getting a target when targets file doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            mock_config_loader.get.return_value = "targets.yaml"
            mock_config_loader.project_dir = tmpdir_path

            with pytest.raises(Exception, match="Targets file not found"):
                get_target("dev")
