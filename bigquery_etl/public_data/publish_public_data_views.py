"""
Generate and publish views for publicly available tables.

Generate view definitions for queries that are written to the
public data project and execute them. Views are published to
an internal project so that data is also accessible in private
datasets.
"""

from argparse import ArgumentParser

from google.cloud import bigquery

from ..util.bigquery_tables import get_tables_matching_patterns
from ..util import standard_args

DEFAULT_PATTERN = "mozilla-public-data:*.*"


parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--target-project",
    default="moz-fx-data-shared-prod",
    help="Create views in the target project",
)
parser.add_argument(
    "patterns",
    metavar="[project:]dataset[.table]",
    default=[DEFAULT_PATTERN],
    nargs="*",
    help="Table that should have a latest-version view, may use shell-style wildcards,"
    f" defaults to: {DEFAULT_PATTERN}",
)
standard_args.add_dry_run(parser)


def generate_and_publish_views(client, tables, target_project, dry_run):
    """Generate view definitions for public data tables and executes them."""
    for public_table in tables:
        project, dataset, table_name = public_table.split(".")
        full_view_id = f"{target_project}.{dataset}.{table_name}"

        view_sql = f"""CREATE OR REPLACE VIEW
            `{full_view_id}`
        AS SELECT * FROM `{public_table}`
        """

        job_config = bigquery.QueryJobConfig(use_legacy_sql=False, dry_run=dry_run)
        client.query(view_sql, job_config)


def main():
    """Publish public data views."""
    args = parser.parse_args()

    client = bigquery.Client(args.target_project)
    tables = get_tables_matching_patterns(client, args.patterns)
    generate_and_publish_views(client, tables, args.target_project, args.dry_run)


if __name__ == "__main__":
    main()
