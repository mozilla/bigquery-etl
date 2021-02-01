"""Validates view definitions."""

from argparse import ArgumentParser
import glob
from multiprocessing.pool import Pool
from pathlib import Path
import re
import sqlparse
import sys

from bigquery_etl.dryrun import DryRun, SKIP as DRYRUN_SKIP
from bigquery_etl.util import standard_args

SKIP = DRYRUN_SKIP | {
    # not matching directory structure, but created before validation was enforced
    "sql/moz-fx-data-shared-prod/stripe/subscription/view.sql",
    "sql/moz-fx-data-shared-prod/stripe/product/view.sql",
    "sql/moz-fx-data-shared-prod/stripe/plan/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/client_probe_counts_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/clients_daily_histogram_aggregates_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/clients_scalar_aggregates_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/clients_daily_scalar_aggregates_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/clients_histogram_aggregates_v1/view.sql",
    "sql/moz-fx-data-shared-prod/search/search_aggregates/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/clients_probe_processes/view.sql",
}

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--sql_dir",
    "--sql-dir",
    help="Directories with view definitions to be validated.",
    default="sql/",
)
standard_args.add_log_level(parser)


def validate_view_naming(view_file):
    """Validate that the created view naming matches the directory structure."""
    with open(view_file) as f:
        sql = f.read()
    parsed = sqlparse.parse(sql)[0]
    tokens = [
        t
        for t in parsed.tokens
        if not (t.is_whitespace or isinstance(t, sqlparse.sql.Comment))
    ]
    is_view_statement = (
        " ".join(tokens[0].normalized.split()) == "CREATE OR REPLACE"
        and tokens[1].normalized == "VIEW"
    )
    if is_view_statement:
        target_view = str(tokens[2]).strip().split()[0]
        try:
            [project_id, dataset_id, view_id] = target_view.replace("`", "").split(".")
            if not (
                view_file.parent.name == view_id
                and view_file.parent.parent.name == dataset_id
                and view_file.parent.parent.parent.name == project_id
            ):
                print(
                    f"{view_file} ERROR\n"
                    f"View name {target_view} not matching directory structure."
                )
                return False
        except Exception:
            print(f"{view_file} ERROR\n{target_view} missing project ID qualifier.")
            return False
    else:
        print(
            f"ERROR: {view_file} does not appear to be "
            "a CREATE OR REPLACE VIEW statement! Quitting..."
        )
        return False
    return True


def validate_fully_qualified_references(view_file):
    """Check that referenced tables and views are fully qualified."""
    with open(view_file) as f:
        sql = f.read()

    # dry run only returns referenced tables, not views
    referenced_tables = DryRun(str(view_file)).get_referenced_tables()
    for line in sqlparse.format(sql, strip_comments=True).split("\n"):
        for referenced_table in referenced_tables:
            # If a view is referenced in the query then instead of the view name,
            # the tables that are referenced it the view definition are returned.
            # Some of these tables are not part of the SQL of the view that is
            # validated.
            ref_dataset = referenced_table[1]
            ref_table = referenced_table[2]
            # check for references to standard view names
            ref_view_dataset = ref_dataset.rsplit("_", 1)[0]
            ref_view = re.match(r"^(.*?)(_v\d+)?$", ref_table)[1]
            for ref_dataset, ref_table in [
                (ref_dataset, ref_table),
                (ref_view_dataset, ref_view),
            ]:
                if re.search(fr"(?<!\.)`?\b{ref_dataset}`?\.`?{ref_table}\b", line):
                    print(
                        f"{view_file} ERROR\n"
                        f"{ref_dataset}.{ref_table} missing project ID qualifier."
                    )
                    return False
        return True


def validate(view_file):
    """Validate UDF docs."""
    view_file = Path(view_file)
    if not validate_view_naming(view_file):
        return False
    if not validate_fully_qualified_references(view_file):
        return False

    print(f"{view_file} OK")
    return True


def main():
    """Validate views."""
    args = parser.parse_args()

    view_files = [
        f
        for f in glob.glob(f"{args.sql_dir}/**/view.sql", recursive=True)
        if f not in SKIP
    ]
    with Pool(8) as p:
        result = p.map(validate, view_files, chunksize=1)
    if all(result):
        exitcode = 0
    else:
        exitcode = 1
    sys.exit(exitcode)


if __name__ == "__main__":
    main()
