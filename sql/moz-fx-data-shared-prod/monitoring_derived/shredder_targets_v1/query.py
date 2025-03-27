#!/usr/bin/env python3

"""Search for tables with client id columns."""
import datetime
from collections import defaultdict
from functools import cache
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set

import click
from google.cloud import bigquery
from google.cloud import datacatalog_lineage_v1 as datacatalog_lineage
from google.cloud.bigquery import TableReference
from google.cloud.exceptions import NotFound

from bigquery_etl.schema import Schema
from bigquery_etl.shredder.config import (
    CLIENT_ID,
    DELETE_TARGETS,
    GLEAN_CLIENT_ID,
    SHARED_PROD,
    DeleteSource,
    find_glean_targets,
    get_glean_channel_to_app_name_mapping,
)

FIND_TABLES_QUERY_TEMPLATE = """
WITH no_client_id_tables AS (
  SELECT
    table_catalog,
    table_schema,
    table_name,
  FROM
    `{project}.region-us.INFORMATION_SCHEMA.TABLE_OPTIONS`
  WHERE
    option_name = 'labels'
    AND option_value LIKE '%(\"include_client_id\", \"false\")%'
)

SELECT
  table_catalog,
  table_schema,
  table_name,
FROM
  `{project}.region-us.INFORMATION_SCHEMA.TABLES`
LEFT JOIN
  no_client_id_tables
USING
  (table_catalog, table_schema, table_name)
WHERE
  -- find tables with columns ending with client_id
  (
    (
      ddl LIKE '%client_id STRING%' AND no_client_id_tables.table_name IS NULL
    )
    OR (
      -- glean tables may have an all null client_info.client_id column but may have a secondary client id
      ddl LIKE '%client_id STRING%client_id STRING%' AND no_client_id_tables.table_name IS NOT NULL
    )
  )
  AND table_type = 'BASE TABLE'  -- exclude views
  AND (
    table_schema LIKE '%_derived'
    or table_schema LIKE '%_stable'
  )
  -- TODO: can't get lineage for most opmon tables, need to figure this out separately
  AND table_schema != "operational_monitoring_derived"
  AND table_schema != "backfills_staging_derived"
  AND table_name != 'deletion_request_v1'
  AND table_name != 'deletion_request_v4'
"""


def find_client_id_tables(project: str) -> List[str]:
    """Return a list of tables that have columns ending with 'client_id'."""
    client = bigquery.Client(project=project)
    row_results = client.query_and_wait(
        query=FIND_TABLES_QUERY_TEMPLATE.format(project=project)
    )

    return [f"{project}.{row.table_schema}.{row.table_name}" for row in row_results]


def get_upstream_stable_tables(id_tables: List[str]) -> Dict[str, Set[str]]:
    """Build map of tables to upstream stable tables using GCP data catalog lineage.

    Note that the data catalog only uses the information from the last 30 days of query jobs.
    """
    client = datacatalog_lineage.LineageClient()

    upstream_stable_tables = defaultdict(set)

    def traverse_upstream(base_table: str):
        """Recursively traverse lineage to find stable tables."""
        table_ref = TableReference.from_string(base_table)

        if table_ref.dataset_id.endswith("_stable"):  # stable tables are terminal nodes
            upstream_stable_tables[base_table] = {base_table}
        elif base_table not in upstream_stable_tables:
            upstream_links_result = client.search_links(
                request={
                    "parent": "projects/moz-fx-data-shared-prod/locations/us",
                    "target": datacatalog_lineage.EntityReference(
                        fully_qualified_name=f"bigquery:{base_table}"
                    ),
                }
            )
            # recursively add upstream tables
            for upstream_link in upstream_links_result:
                # remove "bigquery:" and "sharded:" prefixes
                link_parts = upstream_link.source.fully_qualified_name.split(":")
                source = link_parts[0]
                parent_table = link_parts[-1]
                if source == "gcs":
                    break
                upstream_stable_tables[base_table] = upstream_stable_tables[
                    base_table
                ].union(traverse_upstream(parent_table))

        return upstream_stable_tables[base_table]

    upstream_stable_table_map = {}

    print("Upstream stable tables:")
    for table_name in id_tables:
        upstream_stable_table_map[table_name] = set(traverse_upstream(table_name))
        print(f"{table_name} upstream: {upstream_stable_table_map[table_name]}")

    return upstream_stable_table_map


@cache
def table_exists(client: bigquery.Client, table_name: str) -> bool:
    """Return true if given project.dataset.table exists, caching results."""
    try:
        client.get_table(table_name)
        return True
    except NotFound:
        return False


def get_associated_deletions(
    project: str, upstream_stable_tables: Dict[str, Set[str]]
) -> Dict[str, Set[DeleteSource]]:
    """Get a list of associated deletion requests tables per table based on the stable tables."""
    client = bigquery.Client(project=project)

    # deletion targets for stable tables defined in the shredder config
    known_stable_table_sources: Dict[str, Set[DeleteSource]] = {
        f"{target.project}.{target.dataset_id}.{target.table_id}": (
            set(src) if isinstance(src, Iterable) else {src}
        )
        for target, src in DELETE_TARGETS.items()
        if target.dataset_id.endswith("_stable")
    }

    table_to_deletions: Dict[str, Set[DeleteSource]] = {}

    datasets_with_additional_deletion_requests = {}

    for base_table in upstream_stable_tables:
        if base_table.endswith(".additional_deletion_requests_v1"):
            dataset_name = TableReference.from_string(base_table).dataset_id
            datasets_with_additional_deletion_requests[dataset_name] = (
                f"{dataset_name}.additional_deletion_requests_v1"
            )
            datasets_with_additional_deletion_requests[
                dataset_name.replace("_derived", "_stable")
            ] = f"{dataset_name}.additional_deletion_requests_v1"

    glean_channel_names = get_glean_channel_to_app_name_mapping()

    for table_name, stable_tables in upstream_stable_tables.items():
        deletion_tables: Set[DeleteSource] = set()

        for stable_table in stable_tables:
            if stable_table in known_stable_table_sources:
                table_to_deletions[stable_table] = known_stable_table_sources[
                    stable_table
                ]
            elif stable_table not in table_to_deletions:
                stable_table_ref = TableReference.from_string(stable_table)

                # glean table
                if (
                    stable_table_ref.dataset_id[: -len("_stable")]
                    in glean_channel_names
                ):
                    if table_exists(
                        client,
                        f"{stable_table_ref.project}.{stable_table_ref.dataset_id}.deletion_request_v1",
                    ):
                        table_to_deletions[stable_table] = {
                            DeleteSource(
                                table=f"{stable_table_ref.dataset_id}.deletion_request_v1",
                                field=GLEAN_CLIENT_ID,
                                project=SHARED_PROD,
                            )
                        }
                    else:
                        print(f"No deletion requests for {stable_table_ref.dataset_id}")
                        table_to_deletions[stable_table] = set()

                    if (
                        stable_table_ref.dataset_id
                        in datasets_with_additional_deletion_requests
                    ):
                        table_to_deletions[stable_table].add(
                            DeleteSource(
                                table=datasets_with_additional_deletion_requests[
                                    stable_table_ref.dataset_id
                                ],
                                field=CLIENT_ID,
                                project=SHARED_PROD,
                            )
                        )
                # unknown legacy telemetry or non-glean structured
                else:
                    table_to_deletions[stable_table] = set()

            deletion_tables = deletion_tables.union(table_to_deletions[stable_table])

        table_to_deletions[table_name] = deletion_tables

    return {
        table: deletions
        for table, deletions in table_to_deletions.items()
        if table in upstream_stable_tables
    }


def delete_source_to_dict(source: DeleteSource):
    """Convert a DeleteSource to a dict, removing the condition field."""
    d = source.__dict__.copy()
    d.pop("conditions")
    return d


def get_missing_deletions(
    associated_deletions: Dict[str, Set[DeleteSource]]
) -> List[Dict[str, Any]]:
    """Get list of all tables with the currently configured deletion sources and the sources based on lineage."""
    # get the generated glean deletion list
    with ThreadPool(processes=12) as pool:
        bigquery_client = bigquery.Client()
        glean_delete_targets = find_glean_targets(pool, client=bigquery_client)

    glean_channel_names = get_glean_channel_to_app_name_mapping()
    glean_app_name_to_channels = defaultdict(list)
    for channel, app_name in glean_channel_names.items():
        glean_app_name_to_channels[app_name].append(channel)

    table_deletions = []

    for target, sources in (*glean_delete_targets.items(), *DELETE_TARGETS.items()):
        target_table = f"{target.project}.{target.table}"

        # expand per-app deletion request views into per-channel tables
        unnested_sources = set()
        if isinstance(sources, Iterable):
            for source in sources:
                if (
                    source.dataset_id in glean_app_name_to_channels
                    and source.table_id == "deletion_request"
                ):
                    for channel in glean_app_name_to_channels[source.dataset_id]:
                        unnested_sources.add(
                            DeleteSource(
                                table=f"{channel}_stable.deletion_request_v1",
                                field=source.field,
                                project=source.project,
                                conditions=source.conditions,
                            )
                        )
                else:
                    unnested_sources.add(source)
        else:
            unnested_sources.add(sources)

        # tables not in associated_deletions likely use another id column, e.g. user_id
        detected_deletions = associated_deletions.pop(target_table, set())

        table_deletions.append(
            {
                "project_id": target.project,
                "dataset_id": target.dataset_id,
                "table_id": target.table_id,
                "current_sources": [
                    delete_source_to_dict(source) for source in unnested_sources
                ],
                "detected_sources": [
                    delete_source_to_dict(detected) for detected in detected_deletions
                ],
                "matching_sources": set(unnested_sources) == detected_deletions,
            }
        )

    # delete target not in shredder config
    for table_name, srcs in associated_deletions.items():
        project, dataset, table = table_name.split(".")
        table_deletions.append(
            {
                "project_id": project,
                "dataset_id": dataset,
                "table_id": table,
                "current_sources": [],
                "detected_sources": [
                    delete_source_to_dict(detected) for detected in srcs
                ],
                "matching_sources": len(srcs) == 0,
            }
        )

    return table_deletions


def write_to_bigquery(
    run_date: datetime.datetime,
    target_table: TableReference,
    deletions: List[Dict[str, Any]],
):
    client = bigquery.Client()

    result = client.load_table_from_json(
        json_rows=[
            {"run_date": run_date.date().isoformat(), **deletion}
            for deletion in deletions
        ],
        destination=f"{str(target_table)}${run_date.date().strftime('%Y%m%d')}",
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=Schema.from_schema_file(
                Path(__file__).parent / "schema.yaml"
            ).to_bigquery_schema(),
            time_partitioning=bigquery.TimePartitioning(field="run_date"),
        ),
    ).result()

    print(f"Wrote {result.output_rows} rows to {result.destination}")


@click.command
@click.option(
    "--run-date", type=click.DateTime(), help="The date to write in the output."
)
@click.option(
    "--output-table",
    type=TableReference.from_string,
    metavar="PROJECT.DATASET.TABLE",
    help="Table to write results to in the form of PROJECT.DATASET.TABLE.",
)
@click.option(
    "--project-id",
    default=SHARED_PROD,
    help="BigQuery project to search for client id tables.",
)
def main(run_date, output_table, project_id):
    """Find tables in the given project that could be added to shredder."""
    # TODO: handle other id columns
    client_id_tables = find_client_id_tables(project_id)

    print(f"Found {len(client_id_tables)} client id tables.")

    upstream_stable_tables = get_upstream_stable_tables(client_id_tables)

    associated_deletions = get_associated_deletions(project_id, upstream_stable_tables)

    table_deletions = get_missing_deletions(associated_deletions)

    write_to_bigquery(run_date, output_table, table_deletions)


if __name__ == "__main__":
    main()
