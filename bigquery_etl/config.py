"""bqetl_project.yaml config loader."""

from pathlib import Path

import yaml

ROOT = Path(__file__).parent.parent
BQETL_PROJECT_CONFIG = "bqetl_project.yaml"


class _ConfigLoader:
    project_dir: Path = ROOT
    config_file: str = BQETL_PROJECT_CONFIG

    @property
    def config(self):
        config = getattr(self, "_config", None)
        if config:
            return config

        self._config = yaml.safe_load((self.project_dir / self.config_file).read_text())
        return self._config

    def set_project_dir(self, project_dir: Path):
        """Update the project root directory."""
        self.project_dir = project_dir

    def get(self, *args, fallback=None):
        """Get the config option specified by args."""
        conf = self.config

        for arg in args:
            if arg in conf:
                conf = conf[arg]
            else:
                return fallback

        return conf


ConfigLoader = _ConfigLoader()
