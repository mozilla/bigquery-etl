import os
import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from docker_etl import create_new_job
from docker_etl.file_utils import CI_JOB_NAME, CI_WORKFLOW_NAME


class TestCreateNewJob(TestCase):
    @classmethod
    def template_directory(cls) -> Path:
        return Path(__file__).parent / "test_templates"

    @classmethod
    def default_job_directory(cls) -> Path:
        return cls.template_directory() / "default"

    def test_copy_template_success(self):
        job_name = "_test_job_1"

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch("docker_etl.create_new_job.JOBS_DIR", Path(temp_dir)):
                create_new_job.copy_job_template(job_name, self.default_job_directory())

            self.assertListEqual(os.listdir(temp_dir), [job_name])
            self.assertListEqual(
                os.listdir(os.path.join(temp_dir, job_name)),
                ["default_file"],
            )

    def test_copy_template_already_exists(self):
        job_name = "_test_job_1"

        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(os.path.join(temp_dir, job_name))

            with patch("docker_etl.create_new_job.JOBS_DIR", Path(temp_dir)):
                with self.assertRaises(ValueError):
                    create_new_job.copy_job_template(
                        job_name, self.default_job_directory()
                    )

    def test_add_ci_config(self):
        job_name = "_test_job_1"

        with tempfile.TemporaryDirectory() as temp_dir:
            os.makedirs(os.path.join(temp_dir, job_name))

            with patch("docker_etl.create_new_job.JOBS_DIR", Path(temp_dir)):
                create_new_job.add_ci_config(job_name, self.default_job_directory())

            self.assertSetEqual(
                set(os.listdir(os.path.join(temp_dir, job_name))),
                {CI_JOB_NAME, CI_WORKFLOW_NAME},
            )
