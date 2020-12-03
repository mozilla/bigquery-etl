import sys
from typing import List, Set, Tuple

from docker_etl.file_utils import CI_JOB_NAME, CI_WORKFLOW_NAME, get_job_dirs

REQUIRED_FILES = {CI_JOB_NAME, CI_WORKFLOW_NAME, "README.md", "Dockerfile"}


def check_missing_files() -> List[Tuple[str, Set[str]]]:
    """Check all job directories for missing files."""
    failed_jobs = []
    for job_dir in get_job_dirs():
        files = {content.name for content in job_dir.glob("*") if content.is_file()}
        missing_files = REQUIRED_FILES - files

        if len(missing_files) > 0:
            failed_jobs.append((job_dir.name, missing_files))
            print(f"{job_dir.name} missing files: {', '.join(missing_files)}")

    return failed_jobs


if __name__ == "__main__":
    sys.exit(len(check_missing_files()))
