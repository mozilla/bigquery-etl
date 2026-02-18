import logging
from pathlib import Path
from typing import Optional

import attr
import cattrs
import git
import yaml
from jinja2 import Template

from ..config import ConfigLoader

ROOT = Path(__file__).parent.parent.parent


@attr.s()
class Target:
    """Target configuration for deployment."""

    name: str = attr.ib()
    project_id: str = attr.ib()
    dataset_prefix: Optional[str] = attr.ib(default=None)


def get_target(target: str) -> Target:
    targets_file_name = ConfigLoader.get("default", "targets", fallback="targets.yaml")
    targets_file = ConfigLoader.project_dir / targets_file_name

    if not targets_file.exists():
        raise Exception(f"Targets file not found: {targets_file}")

    try:
        repo = git.Repo(ROOT)
        git_branch = repo.active_branch.name
        git_commit = repo.active_branch.commit.hexsha[:8]
    except Exception:
        logging.warning(
            "Not in a git repository. Using 'unknown' for git.branch and git.commit"
        )
        git_branch = "unknown"
        git_commit = "unknown"

    targets_content = targets_file.read_text()
    template = Template(targets_content)
    rendered_content = template.render(git={"branch": git_branch, "commit": git_commit})

    targets = yaml.safe_load(rendered_content)

    if isinstance(targets, dict) and target in targets:
        return cattrs.structure({**targets[target], "name": target}, Target)

    raise Exception(f"Couldn't find target `{target}` in {targets_file}")
