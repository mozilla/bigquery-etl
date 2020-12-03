from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from docker_etl import verify_files
from docker_etl.file_utils import CI_WORKFLOW_NAME


class TestVerifyFiles(TestCase):
    @patch("docker_etl.file_utils.JOBS_DIR", Path(__file__).parent / "test_jobs")
    def test_missing_files_found(self):
        missing_files = verify_files.check_missing_files()
        self.assertListEqual(
            missing_files,
            [("test_job_1", {CI_WORKFLOW_NAME, "README.md", "Dockerfile"})],
        )
