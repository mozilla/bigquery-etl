#!/usr/bin/env python3

"""
Generate queries combining metrics for each dimension in the exported app store data.

It is expected that exported tables follow a naming convention
of {metric}_by_{dimension} or {metric}_total.
"""

import os
import sys
from argparse import ArgumentParser
from collections import defaultdict

from google.cloud import bigquery

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)
from bigquery_etl.format_sql.formatter import reformat  # noqa E402

SQL_DIR = "sql/moz-fx-data-marketing-prod/"

QUERY_TEMPLATE = """
SELECT
  * EXCEPT ({excepted_fields}),
  {additional_fields}
FROM
  {first_table}
{joined_tables}
WHERE
  {filter}
"""

JOIN_TEMPLATE = """
FULL JOIN
  {table}
  USING ({fields})
"""


def parse_args():
    """Parse command line arguments."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project",
        default="moz-fx-data-marketing-prod",
        help="Project containing exported app store data",
    )
    parser.add_argument(
        "--source-dataset",
        default="apple_app_store_exported",
        help="Dataset containing exported app store data",
    )
    parser.add_argument(
        "--destination-dataset",
        default="apple_app_store",
        help="Dataset to write queries to",
    )
    parser.add_argument("--sql-dir", default=SQL_DIR)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--create-table", action="store_true")
    parser.add_argument(
        "--backfill",
        action="store_true",
        help="If --create-table is set, backfill will populate"
        " tables with data from exported tables",
    )

    return parser.parse_args()


def main(project, source_dataset, destination_dataset, create_table, backfill, dryrun):
    """Generate queries and optionally create the tables in BigQuery."""
    client = bigquery.Client(project=project)

    exported_tables = [
        table.table_id
        for table in client.list_tables(source_dataset)
        if table.table_type == "TABLE"
    ]

    tables_by_dimension = defaultdict(list)
    opt_in_metrics = set()

    # group table names by the dimension it is grouped by
    for table_name in exported_tables:
        if table_name.endswith("_total"):
            dimension = None
        else:
            metric, dimension = table_name.split("_by_")
            if dimension.startswith("opt_in"):
                opt_in_metrics.add(metric)
                dimension = dimension.replace("opt_in_", "")

        tables_by_dimension[dimension].append(table_name)

    for dimension, table_names in tables_by_dimension.items():
        qualified_table_names = [
            f"`{project}.{source_dataset}.{table_name}`" for table_name in table_names
        ]

        if dimension is not None:
            fields = f"date, app_name, {dimension}"
            table_name = f"metrics_by_{dimension}"
            metrics = [table_name.split("_by_")[0] for table_name in table_names]
        else:
            fields = "date, app_name"
            table_name = "metrics_total"
            metrics = [table_name.split("_total")[0] for table_name in table_names]

        join_clauses = [
            JOIN_TEMPLATE.format(table=table_name, fields=fields)
            for table_name in qualified_table_names[1:]
        ]

        # add _opt_in to opt-in metrics
        fields_to_add_opt_in = [
            metric for metric in metrics if metric in opt_in_metrics
        ]
        excepted_fields = ",".join(fields_to_add_opt_in)
        additional_fields = [
            f"{name} AS {name}_opt_in"
            for name in fields_to_add_opt_in
            if name != "rate"
        ]

        # rename rate column to opt_in_rate and
        if "rate" in metrics:
            additional_fields.append("rate AS opt_in_rate")

        query_text = QUERY_TEMPLATE.format(
            excepted_fields=excepted_fields,
            additional_fields=", ".join(additional_fields),
            first_table=qualified_table_names[0],
            joined_tables="\n".join(join_clauses),
            filter="date=@submission_date",
        )
        query_path = os.path.join(SQL_DIR, destination_dataset, table_name, "query.sql")

        if not os.path.exists(os.path.dirname(query_path)):
            os.makedirs(os.path.dirname(query_path))

        with open(query_path, "w") as f:
            print(f"Writing {query_path}")
            f.write(reformat(query_text))
            f.write("\n")

        if create_table:
            query_text = QUERY_TEMPLATE.format(
                excepted_fields=excepted_fields,
                additional_fields=", ".join(additional_fields),
                first_table=qualified_table_names[0],
                joined_tables="\n".join(join_clauses),
                filter="TRUE" if backfill else "FALSE",
            )
            schema_update_options = (
                [] if backfill else [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
            )
            job_config = bigquery.QueryJobConfig(
                use_legacy_sql=False,
                dry_run=dryrun,
                destination=f"{project}.{destination_dataset}.{table_name}",
                schema_update_options=schema_update_options,
                time_partitioning=bigquery.TimePartitioning(field="date"),
                create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                if backfill
                else bigquery.WriteDisposition.WRITE_APPEND,
            )
            print(f"Creating table {table_name}")
            query_job = client.query(query_text, job_config)
            if not dryrun:
                query_job.result()


if __name__ == "__main__":
    args = parse_args()
    main(
        args.project,
        args.source_dataset,
        args.destination_dataset,
        args.create_table,
        args.backfill,
        args.dry_run,
    )
