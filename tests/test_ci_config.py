from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

import yaml

from docker_etl import ci_config


class TestCiConfig(TestCase):
    @patch("docker_etl.file_utils.JOBS_DIR", Path(__file__).parent / "test_jobs")
    @patch("docker_etl.ci_config.CI_DIR", Path(__file__).parent / "test_ci")
    def test_missing_files_found(self):
        ci_config_text = ci_config.update_config(dry_run=True)
        ci_config_parsed = yaml.safe_load(ci_config_text)
        self.assertDictEqual(
            ci_config_parsed,
            {
                "jobs": {
                    "test_job_1_job": {"steps": []},
                    "test_job_2_job": {"steps": []},
                },
                "workflows": {
                    "test_job_2_workflow": {
                        "jobs": [
                            "test_job_2_job",
                        ]
                    }
                },
            },
        )

    def test_validate_yaml(self):
        test_file_dir = Path(__file__).parent / "test_files"
        self.assertTrue(ci_config.validate_yaml(test_file_dir / "valid_yaml.yaml"))
        self.assertFalse(ci_config.validate_yaml(test_file_dir / "invalid_yaml.yaml"))
