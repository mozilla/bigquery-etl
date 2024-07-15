#!/usr/bin/env python3

"""Search for tables with client id columns."""
import csv
from collections import defaultdict
from itertools import chain
from multiprocessing.pool import ThreadPool
from typing import Dict, Iterable, List, Set, Tuple

import click
from google.cloud import bigquery
from google.cloud import datacatalog_lineage_v1 as datacatalog_lineage
from google.cloud.bigquery import TableReference

from .config import (
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
  -- can't get lineage for most opmon tables, need to figure this out separately
  AND table_schema != "operational_monitoring_derived"
  AND table_schema != "backfills_staging_derived"
  AND table_name != 'deletion_request_v1'
  AND table_name != 'deletion_request_v4'
"""


def find_client_id_tables(project: str) -> List[str]:
    """Return a list of tables that have columns ending with 'client_id'."""
    client = bigquery.Client()
    row_results = client.query_and_wait(
        query=FIND_TABLES_QUERY_TEMPLATE.format(project=project)
    )

    return [f"{project}.{row.table_schema}.{row.table_name}" for row in row_results]


def get_upstream_stable_tables(id_tables: List[str]) -> Dict[str, Set[str]]:
    """Build map of tables to upstream stable tables."""
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
                parent_table = upstream_link.source.fully_qualified_name.split(":")[-1]

                upstream_stable_tables[base_table] = upstream_stable_tables[
                    base_table
                ].union(traverse_upstream(parent_table))

        return upstream_stable_tables[base_table]

    upstream_stable_table_map = {}

    for table_name in id_tables:
        upstream_stable_table_map[table_name] = set(traverse_upstream(table_name))

    return upstream_stable_table_map


def get_associated_deletions(
    upstream_stable_tables: Dict[str, Set[str]]
) -> Dict[str, Set[DeleteSource | str]]:
    """Get a list of associated deletion requests tables per table based on the stable tables."""
    # deletion targets for stable tables defined in the shredder config
    known_stable_table_sources: Dict[str, Set[DeleteSource | str]] = {
        f"{target.project}.{target.dataset_id}.{target.table_id}": (
            set(src) if isinstance(src, Iterable) else {src}
        )
        for target, src in DELETE_TARGETS.items()
        if target.dataset_id.endswith("_stable")
    }

    table_to_deletions: Dict[str, Set[DeleteSource | str]] = {}

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
        deletion_tables: Set[DeleteSource | str] = set()

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
                    table_to_deletions[stable_table] = {
                        DeleteSource(
                            table=f"{stable_table_ref.dataset_id}.deletion_request_v1",
                            field=GLEAN_CLIENT_ID,
                            project=SHARED_PROD,
                        )
                    }
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
                # legacy telemetry
                elif stable_table_ref.dataset_id == "telemetry_stable":
                    table_to_deletions[stable_table] = {
                        f"unknown telemetry_stable: {stable_table}"
                    }
                # non-glean structured telemetry
                else:
                    table_to_deletions[stable_table] = {
                        f"unknown non-glean structured: {stable_table}"
                    }

            deletion_tables = deletion_tables.union(table_to_deletions[stable_table])

        table_to_deletions[table_name] = deletion_tables

    return {
        table: deletions
        for table, deletions in table_to_deletions.items()
        if table in upstream_stable_tables
    }


def get_missing_deletions(
    associated_deletions: Dict[str, Set[DeleteSource]]
) -> List[Tuple[str, List[str], List[str]]]:
    """Compare configured shredder targets with list based on lineage and return missing or misconfigured targets.

    Return list of tuples of (table name, deletions based on lineage, current deletion sources)
    """
    # get the generated glean deletion list
    with ThreadPool(processes=12) as pool:
        bigquery_client = bigquery.Client()
        glean_delete_targets = find_glean_targets(pool, client=bigquery_client)

    glean_channel_names = get_glean_channel_to_app_name_mapping()
    glean_app_name_to_channels = defaultdict(list)
    for channel, app_name in glean_channel_names.items():
        glean_app_name_to_channels[app_name].append(channel)

    tables_to_add: List[Tuple[str, List[str], List[str]]] = []

    for target, sources in chain(glean_delete_targets.items(), DELETE_TARGETS.items()):
        target_table = f"{SHARED_PROD}.{target.table}"

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
        if target_table in associated_deletions:
            detected_deletions = associated_deletions.pop(target_table)

            # current config matches lineage
            if detected_deletions == unnested_sources:
                pass
            # no lineage likely means table has no scheduled ETL
            elif len(detected_deletions) == 0:
                tables_to_add.append(
                    (
                        target_table,
                        [],
                        [
                            f"{src.dataset_id}.{src.table_id}: {src.field}"
                            for src in unnested_sources
                        ],
                    )
                )
            # lineage-based deletions does not match actual
            else:
                tables_to_add.append(
                    (
                        target_table,
                        [
                            f"{src.dataset_id}.{src.table_id}: {src.field}"
                            for src in detected_deletions
                        ],
                        [
                            f"{src.dataset_id}.{src.table_id}: {src.field}"
                            for src in unnested_sources
                        ],
                    )
                )

    # delete target not in shredder config
    for table_name, srcs in associated_deletions.items():
        tables_to_add.append(
            (
                table_name,
                [
                    (
                        f"{src.dataset_id}.{src.table_id}: {src.field}"
                        if not isinstance(src, str)
                        else src
                    )
                    for src in srcs
                ],
                [],
            )
        )

    return tables_to_add


@click.command
@click.option(
    "--out-file",
    type=click.Path(dir_okay=False, file_okay=True, writable=True),
    required=False,
    help="File to write the output to.  If not specified, results will only be printed.",
)
@click.option(
    "--project-id",
    default=SHARED_PROD,
    help="BigQuery project to search for client id tables.",
)
def main(out_file, project_id):
    """Find tables in the given project that could be added to shredder."""
    client_id_tables = find_client_id_tables(project_id)

    print(f"Found {len(client_id_tables)} client id tables.")

    upstream_stable_tables = get_upstream_stable_tables(client_id_tables)

    associated_deletions = get_associated_deletions(upstream_stable_tables)

    tables_to_add = get_missing_deletions(associated_deletions)

    print("missing tables:")
    with open(out_file, "w") as f:
        csv.writer(f, delimiter=";").writerows(tables_to_add)
        for target in tables_to_add:
            print(target)


if __name__ == "__main__":
    main()
