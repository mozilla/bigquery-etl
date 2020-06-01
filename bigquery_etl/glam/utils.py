"""Utilities for the GLAM module."""
import json
import subprocess
from collections import namedtuple
from itertools import combinations
from typing import List, Tuple

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


def ping_type_from_table(qualified_table):
    """Return the name of a ping as defined in mozilla-pipeline-schemas.

    Example: org_mozilla_fenix_stable.deletion_request_v1 -> deletion-request
    """
    table_id = qualified_table.split(".")[-1]
    ping_name = table_id.rsplit("_", 1)[0]
    return ping_name.replace("_", "-")


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


def compute_datacube_groupings(
    attributes: List[str], fixed_attributes: List[str] = []
) -> List[List[Tuple[str, bool]]]:
    """Generate the combinations of attributes to be computed.

    These are the combinations that are available to the frontend. Some
    dimensions may be fixed and always required.
    """
    max_combinations = len(attributes)
    result = []
    for subset_size in reversed(range(max_combinations + 1)):
        for grouping in combinations(attributes, subset_size):
            if len(set(fixed_attributes) - set(grouping)) > 0:
                # the fixed attributes are always a subset in the grouping
                continue
            select_expr = []
            for attribute in attributes:
                select_expr.append((attribute, attribute in grouping))
            result.append(select_expr)
    return result


if __name__ == "__main__":
    from pprint import PrettyPrinter

    pp = PrettyPrinter()
    pp.pprint(get_custom_distribution_metadata("fenix"))
