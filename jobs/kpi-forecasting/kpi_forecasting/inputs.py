import argparse
import yaml

from dataclasses import dataclass
from dotmap import DotMap


@dataclass
class CLI:
    """
    Parse command-line arguments. This parser enables users to pass a named
    `config` argument.
    """

    def __post_init__(self) -> None:
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument(
            "-c", "--config", type=str, help="Path to configuration yaml file"
        )
        self.args = self.parser.parse_args()


@dataclass
class YAML:
    """
    Create a data structure from a YAML config filepath. Instead of loading the
    YAML as a dictionary, which requires verbose code to access nested dictionary
    values, this class loads YAML as a dot map. Nested values can be accessed using
    dot notation, like `YAML(<filepath>).data.section.subsection.value`.
    """

    filepath: str

    def __post_init__(self) -> None:
        with open(self.filepath, "r") as f:
            data = yaml.safe_load(f)
        self.data = DotMap(data)
