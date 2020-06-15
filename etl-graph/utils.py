import json
import subprocess
from pathlib import Path
from typing import List, Union


def print_json(data):
    print(json.dumps(data, indent=2))


def ensure_folder(path: Path):
    path.mkdir(parents=True, exist_ok=True)
    return path


def run(command: Union[str, List[str]], **kwargs) -> str:
    """Simple wrapper around subprocess.run that returns stdout and raises exceptions on errors."""
    if isinstance(command, list):
        args = command
    elif isinstance(command, str):
        args = command.split()
    else:
        raise RuntimeError(f"run command is invalid: {command}")

    return (
        subprocess.run(args, stdout=subprocess.PIPE, **{**dict(check=True), **kwargs})
        .stdout.decode()
        .strip()
    )


# NOTE: I could use the google-cloud-bigquery package, but most of my
# development happens in bash. This should be easy to refactor. We can query up
# to 10,000 rows. A more robust way of implementing this is to write the query
# to a destination table, then export the results as json.
def run_query(sql: str, output: Path = None) -> dict:
    result = json.loads(
        run(
            [
                "bq",
                "query",
                "--format=json",
                "--use_legacy_sql=false",
                "--max_rows=10000",
                sql,
            ]
        )
    )
    if output:
        with output.open("w") as fp:
            json.dump(result, fp)
    return result
