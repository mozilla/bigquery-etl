"""Validates view definitions."""

import glob
import sys
from argparse import ArgumentParser
from multiprocessing.pool import Pool
from pathlib import Path

import sqlparse

from bigquery_etl.dependency import extract_table_references
from bigquery_etl.util import standard_args

SKIP = {
    # not matching directory structure, but created before validation was enforced
    "sql/moz-fx-data-shared-prod/stripe/subscription/view.sql",
    "sql/moz-fx-data-shared-prod/stripe/product/view.sql",
    "sql/moz-fx-data-shared-prod/stripe/plan/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/client_probe_counts_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/clients_daily_histogram_aggregates_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/clients_scalar_aggregates_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/clients_daily_scalar_aggregates_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/clients_histogram_aggregates_v1/view.sql",
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
    parsed = sqlparse.parse(view_file.read_text())[0]
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
    for table in extract_table_references(view_file.read_text()):
        if len(table.split(".")) < 3:
            print(f"{view_file} ERROR\n{table} missing project_id qualifier")
            return False
    return True


def validate(view_file):
    """Validate UDF docs."""
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
        Path(f)
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
