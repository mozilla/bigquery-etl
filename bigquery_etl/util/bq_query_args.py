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


class UnsupportedQueryArgumentsError(ValueError):
    """The `bq query` arguments can't be run with the BigQuery client."""


class _JsonTextQueryParameter(bigquery.ScalarQueryParameter):
    """JSON query parameter that sends the given JSON text unchanged, like bq.

    The client serializes JSON parameter values itself, which would send a JSON
    null as SQL NULL and can change numbers when parsing and re-serializing.
    """

    def to_api_repr(self) -> dict:
        """Return the API representation with the JSON text as the value."""
        resource = super().to_api_repr()
        resource["parameterValue"]["value"] = self.value
        return resource


def _parse_flags(args: List[str]) -> dict:
    """Parse `--flag`, `--noflag`, `--flag=value` and `--flag value` arguments into a dict."""
    flags: dict = {name: [] for name in REPEATED_FLAGS}
    tokens = iter(args)
    for arg in tokens:
        if arg == "query":
            continue
        if not arg.startswith("--"):
            raise UnsupportedQueryArgumentsError(f"Unexpected argument `{arg}`")
        name, has_value, value = arg[2:].partition("=")

        if not has_value and (name in REPEATED_FLAGS or name in VALUE_FLAGS):
            value = next(tokens, "")
            if not value or value.startswith("--"):
                raise UnsupportedQueryArgumentsError(f"Missing value for `{arg}`")
            has_value = "="

        if name in BOOLEAN_FLAGS:
            if not has_value:
                flags[name] = True
            elif value.lower() in ("true", "false"):
                flags[name] = value.lower() == "true"
            else:
                raise UnsupportedQueryArgumentsError(
                    f"Unexpected boolean value in `{arg}`"
                )
        elif name.startswith("no") and name[2:] in BOOLEAN_FLAGS and not has_value:
            flags[name[2:]] = False
        elif name in REPEATED_FLAGS:
            flags[name].append(value)
        elif name in VALUE_FLAGS:
            # like bq, the last value wins
            flags[name] = value
        else:
            raise UnsupportedQueryArgumentsError(f"Unsupported flag `{arg}`")
    return flags


def _parse_parameter(parameter: str) -> bigquery.ScalarQueryParameter:
    """Parse a `name:type:value` query parameter; an empty type means STRING."""
    parts = parameter.split(":", 2)
    if len(parts) != 3:
        raise UnsupportedQueryArgumentsError(
            f"Parameter `{parameter}` isn't in the name:type:value format"
        )
    name, type_, value = parts
    type_ = type_.upper() or "STRING"
    if not name:
        raise UnsupportedQueryArgumentsError(
            f"Positional parameter `{parameter}` isn't supported"
        )
    if type_ not in SCALAR_PARAMETER_TYPES:
        raise UnsupportedQueryArgumentsError(
            f"Parameter type `{type_}` in `{parameter}` isn't supported"
        )

    if value == "NULL":
        # like bq, NULL is a null value of any type
        return bigquery.ScalarQueryParameter(name, type_, None)
    if type_ == "JSON":
        return _JsonTextQueryParameter(name, type_, value)
    return bigquery.ScalarQueryParameter(name, type_, value)


def parse_bq_query_args(
    args: List[str],
) -> Tuple[Optional[str], bigquery.QueryJobConfig]:
    """Return the project and job config for `bq query` arguments.

    Raises UnsupportedQueryArgumentsError if the arguments use flags that
    aren't supported here, or if the query results are only shown on the
    console, so the caller can fall back to `bq`.
    """
    flags = _parse_flags(args)

    project = flags.get("project_id")
    destination = flags.get("destination_table")
    dry_run = flags.get("dry_run", False)

    if flags.get("use_legacy_sql", False):
        raise UnsupportedQueryArgumentsError("Legacy SQL isn't supported")
    # Results that are only shown on the console are left to bq, which prints them.
    # With a destination table the client is used and results aren't printed, unlike
    # bq: scheduled queries write to a destination without --format=none and need
    # the client for its credential scopes.
    if not (
        destination
        or dry_run
        or flags.get("format") == "none"
        or flags.get("max_rows") == "0"
    ):
        raise UnsupportedQueryArgumentsError(
            "Query results would be printed, which only bq does"
        )

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
    except ValueError as e:
        raise UnsupportedQueryArgumentsError(
            f"Can't resolve the default dataset or destination table: {e}"
        )

    # like bq, --replace takes precedence over --append_table
    if flags.get("replace"):
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    elif flags.get("append_table"):
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    elif flags.get("replace") is False:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

    job_config.query_parameters = [_parse_parameter(p) for p in flags["parameter"]]

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
