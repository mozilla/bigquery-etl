import json
import subprocess


def get_schema(table: str, project: str = "moz-fx-data-shared-prod"):
    """Return the dictionary representation of the BigQuery table schema.

    This returns types in the legacy SQL format.
    """
    process = subprocess.Popen(
        ["bq", "show", "--schema", "--format=json", f"{project}:{table}"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = process.communicate()
    if process.returncode > 0:
        raise Exception(
            f"Call to bq exited non-zero: {process.returncode}", stdout, stderr
        )
    return json.loads(stdout)
