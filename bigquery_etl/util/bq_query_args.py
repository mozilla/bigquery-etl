"""Translate `bq query` command line flags into a BigQuery client job config."""

from typing import List, Optional, Tuple

from google.cloud import bigquery

SCALAR_PARAMETER_TYPES = {
    "BIGNUMERIC",
    "BOOL",
    "BYTES",
    "DATE",
    "DATETIME",
    "FLOAT64",
    "GEOGRAPHY",
    "INT64",
    "JSON",
    "NUMERIC",
    "STRING",
    "TIME",
    "TIMESTAMP",
}

BOOLEAN_FLAGS = {"append_table", "dry_run", "replace", "use_legacy_sql"}
REPEATED_FLAGS = {"parameter", "schema_update_option"}
VALUE_FLAGS = {
    "clustering_fields",
    "dataset_id",
    "destination_table",
    "format",
    "max_rows",
    "project_id",
    "session_id",
    "time_partitioning_field",
    "time_partitioning_type",
}


def _parse_flags(args: List[str]) -> Optional[dict]:
    """Parse `--flag`, `--noflag`, `--flag=value` and `--flag value` arguments into a dict.

    Returns None if an argument isn't a supported flag.
    """
    flags: dict = {name: [] for name in REPEATED_FLAGS}
    tokens = iter(args)
    for arg in tokens:
        if arg == "query":
            continue
        if not arg.startswith("--"):
            return None
        name, has_value, value = arg[2:].partition("=")

        if not has_value and (name in REPEATED_FLAGS or name in VALUE_FLAGS):
            value = next(tokens, "")
            if not value or value.startswith("--"):
                return None
            has_value = "="

        if name in BOOLEAN_FLAGS:
            if not has_value:
                flags[name] = True
            elif value.lower() in ("true", "false"):
                flags[name] = value.lower() == "true"
            else:
                return None
        elif name.startswith("no") and name[2:] in BOOLEAN_FLAGS and not has_value:
            flags[name[2:]] = False
        elif name in REPEATED_FLAGS and has_value:
            flags[name].append(value)
        elif name in VALUE_FLAGS and has_value:
            # like bq, the last value wins
            flags[name] = value
        else:
            return None
    return flags


def _parse_parameter(parameter: str) -> Optional[bigquery.ScalarQueryParameter]:
    """Parse a `name:type:value` query parameter; an empty type means STRING."""
    parts = parameter.split(":", 2)
    if len(parts) != 3:
        return None
    name, type_, value = parts
    type_ = type_.upper() or "STRING"
    # positional parameters, NULL values and array/struct types are left to bq
    if not name or type_ not in SCALAR_PARAMETER_TYPES or value == "NULL":
        return None
    return bigquery.ScalarQueryParameter(name, type_, value)


def parse_bq_query_args(
    args: List[str],
) -> Optional[Tuple[Optional[str], bigquery.QueryJobConfig]]:
    """Return the project and job config for `bq query` arguments.

    Returns None if the arguments use flags that aren't supported here, or if
    the query results are only shown on the console, so the caller can fall
    back to `bq`.
    """
    flags = _parse_flags(args)
    if flags is None:
        return None

    project = flags.get("project_id")
    destination = flags.get("destination_table")
    dry_run = flags.get("dry_run", False)

    if flags.get("use_legacy_sql", False):
        return None
    if flags.get("replace") and flags.get("append_table"):
        return None
    # Results that are only shown on the console are left to bq, which prints them.
    # With a destination table the client is used and results aren't printed, unlike
    # bq: scheduled queries write to a destination without --format=none and need
    # the client for its credential scopes.
    if not (destination or dry_run or flags.get("format") == "none"):
        return None

    job_config = bigquery.QueryJobConfig(use_legacy_sql=False, dry_run=dry_run)

    try:
        if "dataset_id" in flags:
            job_config.default_dataset = bigquery.DatasetReference.from_string(
                flags["dataset_id"].replace(":", ".", 1), default_project=project
            )
        if destination:
            destination = destination.replace(":", ".", 1)
            # like bq, an unqualified table is in the default dataset
            if "." not in destination and job_config.default_dataset:
                dataset = job_config.default_dataset
                destination = f"{dataset.project}.{dataset.dataset_id}.{destination}"
            job_config.destination = bigquery.TableReference.from_string(
                destination, default_project=project
            )
    except ValueError:
        return None

    if flags.get("append_table"):
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    elif flags.get("replace"):
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    elif flags.get("replace") is False:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

    parameters = [_parse_parameter(p) for p in flags["parameter"]]
    if None in parameters:
        return None
    job_config.query_parameters = parameters

    if flags["schema_update_option"]:
        job_config.schema_update_options = flags["schema_update_option"]
    if "time_partitioning_field" in flags or "time_partitioning_type" in flags:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=flags.get("time_partitioning_type", "DAY"),
            field=flags.get("time_partitioning_field"),
        )
    if "clustering_fields" in flags:
        job_config.clustering_fields = flags["clustering_fields"].split(",")
    if "session_id" in flags:
        job_config.connection_properties = [
            bigquery.ConnectionProperty("session_id", flags["session_id"])
        ]

    return project, job_config
