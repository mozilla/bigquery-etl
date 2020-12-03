from pathlib import Path
from typing import Dict, List

CI_JOB_NAME = "ci_job.yaml"
CI_WORKFLOW_NAME = "ci_workflow.yaml"
CI_JOB_TEMPLATE_NAME = "ci_job.template.yaml"
CI_WORKFLOW_TEMPLATE_NAME = "ci_workflow.template.yaml"

ROOT_DIR = (Path(__file__).parent / "..").resolve()
TEMPLATES_DIR = ROOT_DIR / "templates"
JOBS_DIR = ROOT_DIR / "jobs"


def find_file_in_jobs(filename: str) -> List[Path]:
    """Find all files in job directories matching the given filename."""
    return [obj for obj in JOBS_DIR.glob(f"*/{filename}") if obj.is_file()]


def get_job_dirs() -> List[Path]:
    """Get absolute paths of every directory in the jobs directory"""
    return [path for path in JOBS_DIR.glob("*") if path.is_dir()]


def get_templates() -> Dict[str, Path]:
    """Get mappings of template name to template path."""
    return {path.name: path for path in TEMPLATES_DIR.glob("*") if path.is_dir()}
