import json
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import List, Union

from .config import *


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
    logging.debug(f"running command: {command}")
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
def run_query(sql: str, dest_table: str, output: Path = None, project=PROJECT) -> dict:
    """Run a query, and write a json file containing the query results to the output path"""
    # project is the project where the query takes place
    intermediate = Path(tempfile.mkdtemp())
    qualified_name = f"{PROJECT}:{DATASET}.{dest_table}"
    filename = f"{dest_table}.ndjson"
    blob = f"gs://{BUCKET}/{DATASET}/{filename}"

    run(
        [
            "bq",
            "query",
            f"--project_id={project}",
            "--format=json",
            "--use_legacy_sql=false",
            # ignore the results since we'll extract them from an intermediate table
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
    run(f"gsutil cp {blob} {intermediate}")
    loaded = ndjson_load(intermediate / filename)
    # write json instead of ndjson into the output location
    result = output / filename.replace(".ndjson", ".json")
    with (result).open("w") as fp:
        logging.info(f"writing {result}")
        json.dump(loaded, fp, indent=2)
    return loaded


def qualify(project, dataset, table):
    return f"{project}:{dataset}.{table}"
