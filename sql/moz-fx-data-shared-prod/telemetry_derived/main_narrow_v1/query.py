#!/usr/bin/env python3
"""Generate and run query that splits a ping table up by probe."""
import logging
from datetime import datetime, timedelta
from multiprocessing.pool import ThreadPool

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from bigquery_etl.util.bigquery_id import sql_table_id
from bigquery_etl.util.common import TempDatasetReference

# Maximum unresolved GoogleSQL query length: 1MB
# https://cloud.google.com/bigquery/quotas#query_jobs
MAX_QUERY_SIZE = 1024 * 1024
MAX_QUERY_FIELDS = 200
STANDARD_TYPES = {
    "BOOL": "bool",
    "FLOAT64": "float64",
    "INT64": "int64",
    "STRING": "string",
    "TIMESTAMP": "timestamp",
    "ARRAY<STRUCT<key STRING, value STRING>>": "string_map",
    "ARRAY<STRUCT<key STRING, value INT64>>": "int64_map",
    "ARRAY<STRUCT<key STRING, value BOOL>>": "bool_map",
}
STATIC_PATHS = [
    ("submission_timestamp",),
    ("document_id",),
    ("client_id",),
    ("normalized_channel",),
    ("sample_id",),
]
FORCE_EXTRA_PATHS = [("additional_properties",)]


def _utf8len(string):
    return len(string.encode("utf-8"))


def _type_expr(node):
    result = node.field_type
    if result == "BOOLEAN":
        result = "BOOL"
    if result == "INTEGER":
        result = "INT64"
    if result == "FLOAT":
        result = "FLOAT64"
    if result == "RECORD":
        result = (
            "STRUCT<"
            + ", ".join(f"{field.name} {_type_expr(field)}" for field in node.fields)
            + ">"
        )
    if node.mode == "REPEATED":
        result = f"ARRAY<{result}>"
    return result


def _path_name(path):
    return ".".join(path)


def _path_select(path):
    return ".".join(f"`{p}`" for p in path)


def _select_exprs(
    nodes, static_paths=STATIC_PATHS, prefix: tuple[str, ...] = ()
) -> str | tuple[str, ...]:
    for node in nodes:
        path = (*prefix, node.name)
        if path in static_paths:
            continue  # skip static paths
        if node.field_type == "RECORD" and node.mode != "REPEATED":
            yield from _select_exprs(
                node.fields, static_paths=static_paths, prefix=path
            )
        else:
            _t = _type_expr(node)
            if _t not in STANDARD_TYPES or path in FORCE_EXTRA_PATHS:
                yield path
            else:
                yield (
                    "("
                    + f'"{_path_name(path)}",'
                    + "("
                    + ",".join(
                        (_path_select(path) if _t == t else "NULL")
                        for t in STANDARD_TYPES
                    )
                    + "))"
                )


def _group_paths(paths):
    from itertools import groupby

    return {
        key: _group_paths([path[1:] for path in group if len(path) > 1])
        for key, group in groupby(paths, key=lambda x: x[0])
    }


def _select_as_structs(nested_paths, prefix=()):
    for key, group in nested_paths.items():
        path = *prefix, key
        if group:
            yield (
                "STRUCT("
                + ",".join(_select_as_structs(group, prefix=(*prefix, key)))
                + f") AS `{key}`"
            )
        else:
            yield f"{_path_select(path)} AS `{key}`"


def _get_queries(source, temp_dataset):
    assert source.time_partitioning, "source must be time partitioned"
    assert source.clustering_fields, "source must be clustered"
    partition_filter = (
        f"TIMESTAMP_TRUNC({source.time_partitioning.field},"
        f"{source.time_partitioning.type_})="
        f"TIMESTAMP_TRUNC(@partition_value,{source.time_partitioning.type_})"
    )
    static_paths = STATIC_PATHS[:]
    for field in (source.time_partitioning.field, *source.clustering_fields):
        if (field,) not in static_paths:
            static_paths.append((field,))
    select_exprs = list(_select_exprs(source.schema, static_paths=static_paths))
    # query size is limited, so use the minimum number of characters for spacing
    before_select_exprs = (
        "CREATE TABLE `{dest}` "
        # match partitioning and clustering of the source
        f"PARTITION BY TIMESTAMP_TRUNC({source.time_partitioning.field},"
        f"{source.time_partitioning.type_})"
        f"CLUSTER BY {','.join(source.clustering_fields)} "
        "OPTIONS("
        "partition_expiration_days = CAST('inf' AS FLOAT64),"
        "expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP, INTERVAL 1 DAY)"
        ") "
        "AS "
        "SELECT "
        + "".join(f"{_path_select(path)}," for path in static_paths)
        + "field_name,"
        + "value,"
        + "FROM "
        + f"`{sql_table_id(source)}` "
        + "CROSS JOIN "
        + "UNNEST(ARRAY<STRUCT<"
        + "field_name STRING,"
        + "value STRUCT<"
        + ",".join(
            f"`{name}` {type_expr}" for type_expr, name in STANDARD_TYPES.items()
        )
        + ">>>["
    )
    after_select_exprs = (
        f"]) WHERE {partition_filter} "
        + "AND ("
        + " OR ".join(f"value.`{name}` IS NOT NULL" for name in STANDARD_TYPES.values())
        + ")"
    )
    # temp table names have a consistent length, so generate a throwaway temp table here to
    # determine the formatted length of before_select_exprs
    initial_size = _utf8len(before_select_exprs.format(dest=temp_dataset.temp_table()))
    initial_size += _utf8len(after_select_exprs)
    current_size = initial_size
    current_fields = 0
    current_slice = []
    extra_paths = []
    for i, expr in enumerate(select_exprs):
        if isinstance(expr, tuple):
            extra_paths.append(expr)
            continue
        if (
            (current_size + _utf8len(expr) + 1 > MAX_QUERY_SIZE)
            or (i == len(select_exprs) - 1)
            or (current_fields > MAX_QUERY_FIELDS)
        ):
            # make sure to generate a unique temp table for each query
            yield (
                before_select_exprs.format(dest=temp_dataset.temp_table())
                + ",".join(current_slice)
                + after_select_exprs
            )
            current_slice = []
            current_size = initial_size
            current_fields = 0
        else:
            current_size += 1  # comma between expressions
        current_slice.append(expr)
        current_size += _utf8len(expr)
        current_fields += 1
    # final query collects all the paths that don't fit in a standard type
    yield (
        "SELECT "
        + "".join(f"{_path_select(path)}," for path in static_paths)
        + "".join(f"{expr}," for expr in _select_as_structs(_group_paths(extra_paths)))
        + "FROM "
        + f"`{sql_table_id(source)}` "
        + f"WHERE {partition_filter}"
    )


def _run_query(client, sql, job_config, num_retries=0):
    query_job = client.query(sql, job_config)
    if not query_job.dry_run:
        try:
            query_job.result()
        except BadRequest as e:
            if num_retries <= 0:
                raise
            logging.warn("Encountered bad request, retrying: ", e)
            return _run_query(client, sql, job_config, num_retries - 1)
    return query_job


utc_today = datetime.utcnow().date() - timedelta(days=2)


# TODO accept command line parameters
def main(
    parallelism=4,
    partition_value=f"{utc_today:%F}",
    source_table="moz-fx-data-shared-prod.telemetry_stable.main_v4",
    # destination=f"moz-fx-data-shared-prod.telemetry_derived.main_narrow_v1${utc_today:%Y%m%d}",
    destination=f"relud1.telemetry_derived.main_narrow_v1${utc_today:%Y%m%d}",
    extra_destination=f"relud1.telemetry_derived.main_extra_v1${utc_today:%Y%m%d}",
    temp_dataset=TempDatasetReference.from_string("relud1.tmp"),
):
    client = bigquery.Client()
    source = client.get_table(source_table)
    query_parameters = [
        bigquery.ScalarQueryParameter("partition_value", "TIMESTAMP", partition_value)
    ]
    if "$" in destination:
        partition_suffix = "$" + destination.split("$", 1)[1]
    else:
        partition_suffix = ""
    # run multiple queries and copy-union the result into place like
    # copy-deduplicate, to work within the 1MB query size limit
    *queries, extra_query = _get_queries(source, temp_dataset)
    # TODO consider running these in parallel
    with ThreadPool(parallelism) as pool:
        query_jobs = pool.starmap(
            _run_query,
            (
                (
                    client,
                    query,
                    bigquery.QueryJobConfig(
                        query_parameters=query_parameters,
                    ),
                )
                for query in queries
            ),
            chunksize=1,
        )
    sources = [
        f"{sql_table_id(job.destination)}{partition_suffix}" for job in query_jobs
    ]
    copy_job = client.copy_table(
        sources,
        destination,
        job_config=bigquery.CopyJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        ),
    )
    copy_job.result()
    for job in query_jobs:
        client.delete_table(job.destination)
    _run_query(
        client,
        extra_query,
        job_config=bigquery.QueryJobConfig(
            clustering_fields=source.clustering_fields,
            destination=extra_destination,
            query_parameters=query_parameters,
            time_partitioning=source.time_partitioning,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        ),
    )


if __name__ == "__main__":
    main()
