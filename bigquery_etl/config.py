"""bqetl_project.yaml config loader."""

from functools import cached_property
from pathlib import Path
from typing import Sequence

import yaml

ROOT = Path(__file__).parent.parent
BQETL_PROJECT_CONFIG = "bqetl_project.yaml"


class _ConfigLoader:
    config_files: Sequence[Path]

    def __init__(self, *config_files: Path):
        self.config_files = config_files or self._find_config_files()

    def _find_config_files(self) -> list[Path]:
        config_files = []

        # Include the root config file from a local checkout of this repo where `./bqetl bootstrap`
        # has been run to install the `bigquery_etl` package in editable mode.  This is necessary so
        # that running `bqetl` commands in a local checkout of the `private-bigquery-etl` repo gets
        # all of the settings from the root config file as well.
        root_config_file = ROOT / BQETL_PROJECT_CONFIG
        if root_config_file.exists():
            config_files.append(root_config_file)

        working_directory = Path.cwd()
        if not working_directory.is_relative_to(ROOT):
            for directory in (working_directory, *working_directory.parents):
                possible_config_file = directory / BQETL_PROJECT_CONFIG
                if possible_config_file.exists():
                    config_files.append(possible_config_file)
                    break

        return config_files

    def _update_config(
        self, config: dict, extra_config: dict, path_keys: list[str]
    ) -> None:
        for extra_key, extra_value in extra_config.items():
            current_value = config.get(extra_key)
            if current_value is None:
                config[extra_key] = extra_value
                continue

            current_path_keys = path_keys + [extra_key]
            current_path = ".".join(str(key) for key in current_path_keys)
            current_type = type(current_value)
            extra_type = type(extra_value)

            if (
                (current_type in (dict, list) or extra_type in (dict, list))
                and current_type is not extra_type
                and extra_value is not None
            ):
                raise Exception(
                    f"Type mismatch for `{current_path}` config: {current_type} vs {extra_type}"
                )

            # Merge the extra values into the existing config additively.
            if current_type is dict:
                if extra_value:
                    self._update_config(current_value, extra_value, current_path_keys)
            elif current_type is list:
                if extra_value:
                    current_value.extend(extra_value)
            else:
                config[extra_key] = extra_value

    @cached_property
    def config(self) -> dict:
        """Config, lazily loaded from the project config file(s)."""
        config: dict = {}

        if not self.config_files:
            raise FileNotFoundError(
                f"No `{BQETL_PROJECT_CONFIG}` config file was found"
            )

        for config_file in self.config_files:
            extra_config: dict = yaml.safe_load(config_file.read_text())
            if not extra_config:
                continue
            elif not config:
                config = extra_config
            else:
                try:
                    self._update_config(config, extra_config, [])
                except Exception as e:
                    raise Exception(f"Error merging config from `{config_file}`") from e

        return config

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
