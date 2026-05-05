#!/usr/bin/env python3

"""Raise exception when there are potential new targets to add to shredder."""
from textwrap import dedent

import click
from google.cloud import bigquery
from google.cloud.bigquery import TableReference
from typing import Dict, List

NEW_TARGETS_QUERY_TEMPLATE = """
SELECT
    *
FROM
    `{source_table}`
WHERE
    run_date = '{run_date}'
"""


def format_sources(sources: List[Dict[str, str]]) -> List[str]:
    return [f"{source['table']}: {source['field']}" for source in sources]


@click.command
@click.option(
    "--run-date", type=click.DateTime(), help="The date to to check for new targets."
)
@click.option(
    "--source-table",
    type=TableReference.from_string,
    metavar="PROJECT.DATASET.TABLE",
    default="moz-fx-data-shared-prod.monitoring_derived.shredder_targets_new_mismatched_v1",
    help="Table to get new shredder targets from, in the form of PROJECT.DATASET.TABLE. "
    "Defaults to "
    "`moz-fx-data-shared-prod.monitoring_derived.shredder_targets_new_mismatched_v1`",
)
def main(run_date, source_table):
    """Find tables in the given project that could be added to shredder."""
    client = bigquery.Client()

    mismatched_tables = list(
        client.query_and_wait(
            NEW_TARGETS_QUERY_TEMPLATE.format(
                source_table=source_table, run_date=run_date.date()
            )
        )
    )

    table_names = []

    for table in mismatched_tables:
        table_name = f"{table.project_id}.{table.dataset_id}.{table.table_id}"
        table_names.append(table_name)
        print(
            dedent(
                f"""
                {table.project_id}.{table.dataset_id}.{table.table_id}: 
                created on {table.table_creation_date}, owners: {table.owners}
                current deletion sources:          {format_sources(table.current_sources)}
                deletion sources based on lineage: {format_sources(table.detected_sources)}
                """
            )
        )

    if len(table_names) > 0:
        raise RuntimeError(
            dedent(
                f"""
                Found {len(table_names)} new tables that may not be configured correctly in shredder: {table_names}
                Verify with table owners if these tables need to be configured in shredder.
                See logs or {source_table} with run_date="{run_date.date()}" for more details.
                """
            )
        )


if __name__ == "__main__":
    main()
