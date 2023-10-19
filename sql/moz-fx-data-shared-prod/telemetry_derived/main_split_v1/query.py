#!/usr/bin/env python3
"""Generate and run query that splits a ping table up by probe."""
import logging
import re
from datetime import datetime, timedelta
from itertools import groupby
from multiprocessing.pool import ThreadPool
from pathlib import Path

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

from bigquery_etl.util.bigquery_id import sql_table_id
from bigquery_etl.util.common import TempDatasetReference
from bigquery_etl.format_sql.formatter import reformat

STATIC_PATHS = [
    ("submission_timestamp",),
    ("document_id",),
    ("client_id",),
    ("normalized_app_name",),
    ("normalized_channel",),
    ("normalized_os",),
    ("normalized_os_version",),
    ("normalized_country_code",),
    ("sample_id",),
]


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


def _unnest_paths(fields, *prefix: tuple[str, ...], static_paths=STATIC_PATHS):
    for field in fields:
        path = (*prefix, field.name)
        if path in STATIC_PATHS:
            continue  # skip static paths
        elif field.field_type == "RECORD" and field.mode != "REPEATED":
            yield from _unnest_paths(field.fields, *path, static_paths=static_paths)
        else:
            yield (*prefix, field.name)


def _nest_paths(paths):
    return {
        key: _nest_paths([path[1:] for path in group if len(path) > 1])
        for key, group in groupby(paths, key=lambda x: x[0])
    }


def _reduce_remainder(splits, remainder):
    for key, group in list(remainder.items()):
        match_splits = [split[key] for split in splits if key in split]
        if match_splits:
            _reduce_remainder(match_splits, group)
        else:
            remainder[key] = {}  # select whole field in remainder


def _select_as_structs(nested_paths, prefix=()):
    for key, group in nested_paths.items():
        path = *prefix, key
        if group:
            yield (
                "STRUCT("
                + ",".join(_select_as_structs(group, prefix=(*prefix, key)))
                + f") AS `{key}`"
            )
        elif path[-1] == key:
            yield f"{_path_select(path)}"
        else:
            yield f"{_path_select(path)} AS `{key}`"


def _get_queries(source):
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
    paths = list(_unnest_paths(source.schema, static_paths=static_paths))
    use_counter_paths = []
    remainder_paths = []
    pattern = re.compile(".*[.](USE_COUNTER2_[^.]+|((TOP_LEVEL_)?CONTENT_DOCUMENTS|[^.]+_WORKER)_DESTROYED)".lower())
    for path in paths:
        if pattern.fullmatch(".".join(path)):
            use_counter_paths.append(path)
        else:
            remainder_paths.append(path)
    use_counter = _nest_paths(use_counter_paths)
    remainder = _nest_paths(remainder_paths)
    _reduce_remainder([use_counter], remainder)
    # final query collects all the paths that don't fit in a standard type
    return {
        dest: reformat(
            "SELECT "
            + "".join(f"{_path_select(path)}," for path in static_paths)
            + "".join(f"{expr}," for expr in _select_as_structs(paths))
            + "FROM "
            + f"`{sql_table_id(source)}` "
            + f"WHERE {partition_filter} "
            + "AND sample_id = @sample_id"
        )
        for dest, paths in {
            "main_use_counter_v4": use_counter,
            "main_v5": remainder,
        }.items()
    }


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


def main(
    parallelism=2,
    partition_date=utc_today,
    source_table="moz-fx-data-shared-prod.telemetry_stable.main_v4",
    project="moz-fx-data-shared-prod",
    dataset="telemetry_stable",
):
    client = bigquery.Client()
    source = client.get_table(source_table)
    for dest, query in _get_queries(source).items():
        path = (Path("sql") / project / dataset / dest / "query.sql")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(query)
    return
    queries = _get_queries(source)
    query_parameters = [
        bigquery.ScalarQueryParameter(
            "partition_value", "TIMESTAMP", f"{partition_date:%F}"
        )
    ]
    with ThreadPool(parallelism) as pool:
        query_jobs = pool.starmap(
            _run_query,
            (
                (
                    client,
                    query,
                    bigquery.QueryJobConfig(
                        clustering_fields=source.clustering_fields,
                        destination=f"{project}.{dataset}.{dest}${partition_date:%Y%m%d}",
                        query_parameters=query_parameters,
                        time_partitioning=source.time_partitioning,
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                        schema_update_options=[
                            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                        ],
                    ),
                )
                for dest, query in queries.items()
            ),
            chunksize=1,
        )


if __name__ == "__main__":
    for i in range(6):
        main(partition_date=utc_today - timedelta(days=i+1))
