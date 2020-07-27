#!/usr/bin/env python3

"""
Generate views combining metrics for each dimension in the exported app store data.

It is expected that exported tables follow a naming convention
of {metric}_by_{dimension} or {metric}_total.
"""

import os
import sys
from argparse import ArgumentParser
from collections import defaultdict

from google.cloud import bigquery

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from bigquery_etl.format_sql.formatter import reformat  # noqa E402

SQL_DIR = "sql/"

VIEW_TEMPLATE = """
CREATE OR REPLACE VIEW
  {view_name}
AS
SELECT
  * {excepted_cols}
FROM
  {first_table}
{joined_tables}
ORDER BY
  {order_fields}
"""

JOIN_TEMPLATE = """
FULL JOIN
  {table}
USING
  ({fields})
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
        help="Dataset to write views to",
    )
    parser.add_argument("--sql-dir", default=SQL_DIR)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--publish", action="store_true")

    return parser.parse_args()


def main(project, source_dataset, destination_dataset, publish, dryrun):
    """Generate view files and optionally create them in BigQuery."""
    client = bigquery.Client(project=project)

    exported_tables = [
        table.table_id
        for table in client.list_tables(source_dataset)
        if table.table_type == "TABLE"
    ]

    tables_by_dimension = defaultdict(list)

    for table_name in exported_tables:
        if table_name.endswith("_total"):
            dimension = None
        else:
            _, dimension = table_name.split("_by_")

        tables_by_dimension[dimension].append(table_name)

    for dimension, table_names in tables_by_dimension.items():
        qualified_table_names = [
            f"`{project}.{source_dataset}.{table_name}`" for table_name in table_names
        ]

        if dimension is not None:
            dimension = dimension.replace("opt_in_", "")
            fields = f"date, app_name, {dimension}"
            view_name = f"metrics_by_{dimension}"
        else:
            fields = "date, app_name"
            view_name = "metrics_total"

        join_clauses = [
            JOIN_TEMPLATE.format(table=table_name, fields=fields)
            for table_name in qualified_table_names[1:]
        ]

        # rename rate column to opt_in_rate
        if len(list(filter(lambda name: name.startswith("rate_"), table_names))) > 0:
            excepted_cols = "EXCEPT (rate), rate AS opt_in_rate"
        else:
            excepted_cols = ""

        view_text = VIEW_TEMPLATE.format(
            view_name=f"`{project}.{destination_dataset}.{view_name}`",
            excepted_cols=excepted_cols,
            first_table=qualified_table_names[0],
            joined_tables="\n".join(join_clauses),
            order_fields=fields,
        )
        view_path = os.path.join(SQL_DIR, destination_dataset, view_name, "view.sql")

        if not os.path.exists(os.path.dirname(view_path)):
            os.makedirs(os.path.dirname(view_path))

        with open(view_path, "w") as f:
            print(f"Writing {view_path}")
            f.write(reformat(view_text))
            f.write("\n")

        if publish:
            job_config = bigquery.QueryJobConfig(use_legacy_sql=False, dry_run=dryrun)
            print(f"Publishing view {view_name}")
            client.query(view_text, job_config)


if __name__ == "__main__":
    args = parse_args()
    main(
        args.project,
        args.source_dataset,
        args.destination_dataset,
        args.publish,
        args.dry_run,
    )
