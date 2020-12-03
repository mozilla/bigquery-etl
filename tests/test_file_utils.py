from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from docker_etl import file_utils


class TestCreateNewJob(TestCase):
    @classmethod
    def template_directory(cls) -> Path:
        return Path(__file__).parent / "test_templates"

    @classmethod
    def default_template_directory(cls) -> Path:
        return cls.template_directory() / "default"

    @classmethod
    def job_directory(cls) -> Path:
        return Path(__file__).parent / "test_jobs"

    def test_get_templates(self):
        with patch("docker_etl.file_utils.TEMPLATES_DIR", self.template_directory()):
            self.assertDictEqual(
                file_utils.get_templates(),
                {
                    "default": self.default_template_directory(),
                },
            )

    def test_get_job_dirs(self):
        with patch("docker_etl.file_utils.JOBS_DIR", self.job_directory()):
            self.assertListEqual(
                sorted(file_utils.get_job_dirs()),
                [
                    self.job_directory() / "test_job_1",
                    self.job_directory() / "test_job_2",
                ],
            )

    def test_find_file_in_jobs(self):
        with patch("docker_etl.file_utils.JOBS_DIR", self.job_directory()):
            self.assertListEqual(
                sorted(file_utils.find_file_in_jobs("Dockerfile")),
                [self.job_directory() / "test_job_2" / "Dockerfile"],
            )
