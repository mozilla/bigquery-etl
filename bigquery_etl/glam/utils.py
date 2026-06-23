"""Utilities for the GLAM module."""

import json
import subprocess
from collections import namedtuple
from itertools import combinations
from typing import List, Optional, Tuple

import requests
from mozilla_schema_generator.glean_ping import GleanPing

CustomDistributionMeta = namedtuple(
    "CustomDistributionMeta",
    ["name", "type", "range_min", "range_max", "bucket_count", "histogram_type"],
)

# ETL product (stable-table dataset prefix) -> canonical Glean app name.
_PRODUCT_TO_APP_NAME = {
    "firefox_desktop": "firefox_desktop",
    # Experimenter doesn't distinguish Fenix variants; all map to appName=fenix.
    # Variants follow the Play Store app-id history
    # (see https://github.com/mozilla/telemetry-airflow/blob/2caa0865f017afa0743c94df02666c9af3d66862/dags/glam_fenix.py#L47-L56).
    "org_mozilla_fenix": "fenix",
    "org_mozilla_fenix_nightly": "fenix",
    "org_mozilla_firefox": "fenix",
    "org_mozilla_firefox_beta": "fenix",
    "org_mozilla_fennec_aurora": "fenix",
}

# Glean Dictionary is the source of truth for static labeled_counter labels: it is
# more complete/fresh than GleanPing.get_probes() (which lags for recent or
# dependency-sourced metrics, e.g. it omits dns.trr_http3_0rtt_state). The
# per-metric file name matches the BigQuery column form, so stable-table schema
# field names look up directly. The Dictionary uses the same app names as
# Experimenter, so the product -> app mapping uses _PRODUCT_TO_APP_NAME.
GLEAN_DICTIONARY_DATA_URL = "https://dictionary.telemetry.mozilla.org/data"

_glean_metric_metadata_cache: dict = {}


def run(command, **kwargs) -> str:
    """Return the result of stdout and raise on subprocess error."""
    if isinstance(command, list):
        args = command
    elif isinstance(command, str):
        args = command.split()
    else:
        raise RuntimeError(f"run command is invalid: {command}")

    try:
        res = (
            subprocess.run(args, stdout=subprocess.PIPE, check=True, **kwargs)
            .stdout.decode()
            .strip()
        )
    except subprocess.CalledProcessError as e:
        print(e.output.decode())
        raise e

    return res


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
    glean = GleanPing(dict(name=product_name, app_id=product_name))
    probes = glean.get_probes()

    custom = []
    for probe in probes:
        # We use endswith here to accommodate for types prefixed with "labeled"
        if not probe.get_type().endswith("custom_distribution"):
            continue
        meta = CustomDistributionMeta(
            probe.get_name(),
            probe.get_type(),
            probe.get("range_min"),
            probe.get("range_max"),
            probe.get("bucket_count"),
            probe.get("histogram_type"),
        )
        custom.append(meta)

    return custom


def get_glean_metric_metadata(
    product: Optional[str], probe_name: str
) -> Optional[dict]:
    """Return Glean Dictionary metadata for a metric, or None if it is absent.

    None means the metric is genuinely not in the Dictionary (HTTP 404) or the
    product is unmapped. Network errors, timeouts and non-404 HTTP statuses are
    raised rather than swallowed: treating them as "absent" would silently route
    a static labeled_counter into the scalar pipeline, and the scalar and
    histogram generators look this up independently, so an intermittent outage
    could land a metric in both pipelines or neither.
    """
    cache_key = (product, probe_name)
    if cache_key in _glean_metric_metadata_cache:
        return _glean_metric_metadata_cache[cache_key]

    app = _PRODUCT_TO_APP_NAME.get(product) if product is not None else None
    if app is None:
        _glean_metric_metadata_cache[cache_key] = None
        return None

    url = f"{GLEAN_DICTIONARY_DATA_URL}/{app}/metrics/data_{probe_name}.json"
    resp = requests.get(url, timeout=10)
    if resp.status_code == 404:
        meta = None
    else:
        # Anything other than 404 must fail loudly, not be treated as absent.
        resp.raise_for_status()
        info = resp.json()
        meta = {"type": info.get("type"), "labels": info.get("labels") or []}

    _glean_metric_metadata_cache[cache_key] = meta
    return meta


def is_static_labeled_counter(product: Optional[str], probe_name: str) -> bool:
    """Return whether a metric is a labeled_counter with predefined (static) `labels`.

    These are Glean's replacement for Legacy Telemetry categorical histograms and
    are processed in the histogram pipeline; dynamic (open-ended) labeled_counters
    have no static `labels` and stay in the scalar pipeline.
    """
    meta = get_glean_metric_metadata(product, probe_name)
    if not meta:
        return False
    return meta.get("type") == "labeled_counter" and bool(meta.get("labels"))


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
