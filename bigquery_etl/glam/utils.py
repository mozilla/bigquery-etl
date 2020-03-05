"""Utilities for the GLAM module."""
import json
import subprocess
from collections import namedtuple
from typing import List

from mozilla_schema_generator.glean_ping import GleanPing

CustomDistributionMeta = namedtuple(
    "CustomDistributionMeta",
    ["name", "range_min", "range_max", "bucket_count", "histogram_type"],
)


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


def get_custom_distribution_metadata(product_name) -> List[CustomDistributionMeta]:
    """Get metadata for reconstructing custom distribution buckets in Glean metrics."""
    # GleanPing.get_repos -> List[Tuple[name: str, app_id: str]]
    glean = GleanPing(product_name)
    probes = glean.get_probes()

    custom = []
    for probe in probes:
        if probe.get_type() != "custom_distribution":
            continue
        meta = CustomDistributionMeta(
            probe.get_name(),
            probe.get("range_min"),
            probe.get("range_max"),
            probe.get("bucket_count"),
            probe.get("histogram_type"),
        )
        custom.append(meta)

    return custom


if __name__ == "__main__":
    from pprint import PrettyPrinter

    pp = PrettyPrinter()
    pp.pprint(get_custom_distribution_metadata("fenix"))
