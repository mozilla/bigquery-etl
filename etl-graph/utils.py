import json
import subprocess
from pathlib import Path
from typing import List, Union
from .config import *


def print_json(data):
    print(json.dumps(data, indent=2))


def ensure_folder(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def ndjson_load(path: Path) -> List[dict]:
    data = []
    with path.open() as fp:
        for line in fp.readlines():
            data.append(json.loads(line))
    return data


def run(command: Union[str, List[str]], **kwargs) -> str:
    """Simple wrapper around subprocess.run that returns stdout and raises exceptions on errors."""
    if isinstance(command, list):
        args = command
    elif isinstance(command, str):
        args = command.split()
    else:
        raise RuntimeError(f"run command is invalid: {command}")

    try:
        result = (
            subprocess.run(
                args, stdout=subprocess.PIPE, **{**dict(check=True), **kwargs}
            )
            .stdout.decode()
            .strip()
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"command {exc.cmd} failed with status {exc.returncode}: {exc.output.decode()}"
        )
    return result


# NOTE: I could use the google-cloud-bigquery package, but most of my
# development happens in bash.
def run_query(sql: str, dest_table: str, output: Path = None) -> dict:
    qualified_name = f"{PROJECT}:{DATASET}.{dest_table}"
    filename = f"{dest_table}.ndjson"
    blob = f"gs://{BUCKET}/{DATASET}/{filename}"

    run(
        [
            "bq",
            "query",
            "--format=json",
            "--use_legacy_sql=false",
            "--max_rows=0",
            f"--destination_table={qualified_name}",
            "--replace",
            sql,
        ]
    )
    run(
        [
            "bq",
            "extract",
            "--destination_format=NEWLINE_DELIMITED_JSON",
            qualified_name,
            blob,
        ]
    )
    run(f"gsutil cp {blob} {output}")
    return ndjson_load(output / filename)
