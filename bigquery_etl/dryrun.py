"""
Dry run query files.

Passes all queries to a Cloud Function that will run the
queries with the dry_run option enabled.

We could provision BigQuery credentials to the CircleCI job to allow it to run
the queries directly, but there is no way to restrict permissions such that
only dry runs can be performed. In order to reduce risk of CI or local users
accidentally running queries during tests and overwriting production data, we
proxy the queries through the dry run service endpoint.
"""
import fnmatch
import glob
import json
import re
import sys
from argparse import ArgumentParser
from enum import Enum
from functools import cached_property
from multiprocessing.pool import Pool
from os.path import basename, dirname, exists, isdir
from typing import Set
from urllib.request import Request, urlopen

SKIP = {
    # Access Denied
    "sql/moz-fx-data-shared-prod/account_ecosystem_derived/ecosystem_client_id_lookup_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/account_ecosystem_derived/desktop_clients_daily_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/account_ecosystem_restricted/ecosystem_client_id_deletion_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/account_ecosystem_derived/fxa_logging_users_daily_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/activity_stream/impression_stats_flat/view.sql",
    "sql/moz-fx-data-shared-prod/activity_stream/tile_id_types/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/deletion_request_volume_v1/query.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/document_sample_nonprod_v1/query.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/schema_error_counts_v1/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/schema_error_counts_v2/query.sql",
    "sql/moz-fx-data-shared-prod/monitoring/schema_error_counts_v1/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/structured_error_counts_v1/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring/structured_error_counts/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/telemetry_missing_columns_v1/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring/telemetry_missing_columns_v1/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/telemetry_missing_columns_v2/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring/telemetry_missing_columns_v2/view.sql",
    "sql/moz-fx-data-shared-prod/pocket/pocket_reach_mau/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/buildhub2/view.sql",
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_content_events_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_auth_bounce_events_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_auth_events_v1/query.sql",
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_delete_events_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_delete_events_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_oauth_events_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_log_auth_events_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_log_content_events_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_log_device_command_events_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_users_services_first_seen_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_users_services_last_seen_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/addons_daily_v1/query.sql",
    "sql/moz-fx-data-shared-prod/search_derived/search_clients_last_seen_v1/init.sql",
    "sql/moz-fx-data-shared-prod/search_derived/search_clients_last_seen_v1/query.sql",
    "sql/moz-fx-data-shared-prod/search/search_rfm/view.sql",
    "sql/moz-fx-data-shared-prod/search/search_clients_last_seen_v1/view.sql",
    "sql/moz-fx-data-shared-prod/search/search_clients_last_seen/view.sql",
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_amplitude_export_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_amplitude_user_ids_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_amplitude_user_ids_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/regrets_reporter/regrets_reporter_update/view.sql",
    "sql/moz-fx-data-shared-prod/revenue_derived/client_ltv_v1/query.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/shredder_progress/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring/shredder_progress/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/telemetry_distinct_docids_v1/query.sql",
    "sql/moz-fx-data-shared-prod/revenue_derived/client_ltv_normalized/query.sql",
    "sql/moz-fx-data-shared-prod/stripe_derived/customers_v1/query.sql",
    "sql/moz-fx-data-shared-prod/stripe_derived/plans_v1/query.sql",
    "sql/moz-fx-data-shared-prod/stripe_derived/products_v1/query.sql",
    "sql/moz-fx-data-shared-prod/stripe_derived/subscriptions_v1/query.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/charges_v1/query.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/credit_notes_v1/query.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/customers_v1/query.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/disputes_v1/query.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/invoices_v1/query.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/payment_intents_v1/query.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/payouts_v1/query.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/plans_v1/query.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/prices_v1/query.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/products_v1/query.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/setup_intents_v1/query.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/subscriptions_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/add_device_events_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/devices_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/login_flows_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/protected_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/subscriptions_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/users_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/waitlist_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/devices_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/subscriptions_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/users_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/waitlist_v1/query.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/telemetry_missing_columns_v3/query.sql",
    "sql/moz-fx-data-experiments/monitoring/query_cost_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/subscriptions_v1/init.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/waitlist_v1/init.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/users_v1/init.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/protected_v1/init.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/add_device_events_v1/init.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/devices_v1/init.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/charges_v1/init.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/payouts_v1/init.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/subscriptions_v1/init.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/payment_intents_v1/init.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/products_v1/init.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/disputes_v1/init.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/credit_notes_v1/init.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/customers_v1/init.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/invoices_v1/init.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/setup_intents_v1/init.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/plans_v1/init.sql",
    "sql/moz-fx-data-shared-prod/stripe_external/prices_v1/init.sql",
    "sql/moz-fx-data-bq-performance/release_criteria/dashboard_health_v1/query.sql",
    "sql/moz-fx-data-bq-performance/release_criteria/rc_flattened_test_data_v1/query.sql",
    "sql/moz-fx-data-bq-performance/release_criteria/release_criteria_summary_v1/query.sql",
    "sql/moz-fx-data-bq-performance/release_criteria/stale_tests_v1/query.sql",
    "sql/moz-fx-data-bq-performance/release_criteria/release_criteria_v1/query.sql",
    # Already exists (and lacks an "OR REPLACE" clause)
    "sql/moz-fx-data-shared-prod/org_mozilla_firefox_derived/clients_first_seen_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/org_mozilla_firefox_derived/clients_last_seen_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/org_mozilla_fenix_derived/clients_last_seen_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/org_mozilla_vrbrowser_derived/clients_last_seen_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/core_clients_last_seen_v1/init.sql",
    "sql/moz-fx-data-shared-prod/telemetry/fxa_users_last_seen_raw_v1/init.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/core_clients_first_seen_v1/init.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/fxa_users_services_last_seen_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/messaging_system_derived/cfr_users_last_seen_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/messaging_system_derived/onboarding_users_last_seen_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/messaging_system_derived/snippets_users_last_seen_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/messaging_system_derived/whats_new_panel_users_last_seen_v1/init.sql",  # noqa E501
    # Reference table not found
    "sql/moz-fx-data-shared-prod/monitoring_derived/structured_detailed_error_counts_v1/view.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/monitoring/structured_detailed_error_counts/view.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/org_mozilla_firefox_derived/migrated_clients_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/org_mozilla_firefox_derived/incline_executive_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/org_mozilla_firefox/migrated_clients/view.sql",
    # No matching signature for function IF
    "sql/moz-fx-data-shared-prod/static/fxa_amplitude_export_users_last_seen/query.sql",
    # Duplicate UDF
    "sql/moz-fx-data-shared-prod/static/fxa_amplitude_export_users_daily/query.sql",
    # Syntax error
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_last_seen_v1/init.sql",  # noqa E501
    # HTTP Error 408: Request Time-out
    "sql/moz-fx-data-shared-prod/telemetry_derived/latest_versions/query.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/italy_covid19_outage_v1/query.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/main_nightly_v1/init.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/main_1pct_v1/init.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/main_1pct_v1/query.sql",
    # Query parameter not found
    "sql/moz-fx-data-shared-prod/telemetry_derived/experiments_v1/query.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_scalar_aggregates_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_keyed_scalar_aggregates_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_keyed_boolean_aggregates_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_histogram_aggregates_content_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_histogram_aggregates_parent_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_keyed_histogram_aggregates_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_histogram_aggregates_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_histogram_bucket_counts_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/glam_client_probe_counts_extract_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/scalar_percentiles_v1/query.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_scalar_probe_counts_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/asn_aggregates_v1/query.sql",
    # Dataset sql/glam-fenix-dev:glam_etl was not found
    *glob.glob("sql/glam-fenix-dev/glam_etl/**/*.sql", recursive=True),
    # Query templates
    "sql/moz-fx-data-shared-prod/search_derived/mobile_search_clients_daily_v1/fenix_metrics.template.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/search_derived/mobile_search_clients_daily_v1/mobile_search_clients_daily.template.sql",  # noqa E501
}


class Errors(Enum):
    """DryRun errors that require special handling."""

    READ_ONLY = 1
    DATE_FILTER_NEEDED = 2
    DATE_FILTER_NEEDED_AND_SYNTAX = 3


class DryRun:
    """Dry run SQL files."""

    # todo: support different dry run endpoints for different projects
    DRY_RUN_URL = (
        "https://us-central1-moz-fx-data-shared-prod.cloudfunctions.net/"
        "bigquery-etl-dryrun"
    )

    def __init__(self, sqlfile, content=None, strip_dml=False):
        """Instantiate DryRun class."""
        self.sqlfile = sqlfile
        self.content = content
        self.strip_dml = strip_dml

    def get_sql(self):
        """Get SQL content."""
        if exists(self.sqlfile):
            sql = open(self.sqlfile).read()
        else:
            raise ValueError(f"Invalid file path: {self.sqlfile}")
        if self.strip_dml:
            sql = re.sub(
                "CREATE OR REPLACE VIEW.*?AS",
                "",
                sql,
                flags=re.DOTALL,
            )

        return sql

    @cached_property
    def dry_run_result(self):
        """Dry run the provided SQL file."""
        if self.content:
            sql = self.content
        else:
            sql = self.get_sql()
        try:
            r = urlopen(
                Request(
                    self.DRY_RUN_URL,
                    headers={"Content-Type": "application/json"},
                    data=json.dumps(
                        {
                            "dataset": basename(dirname(dirname(self.sqlfile))),
                            "query": sql,
                        }
                    ).encode("utf8"),
                    method="POST",
                )
            )
        except Exception as e:
            print(f"{self.sqlfile:59} ERROR\n", e)
            return None

        return json.load(r)

    def get_referenced_tables(self):
        """Return referenced tables by dry running the SQL file."""
        if self.sqlfile not in SKIP and not self.is_valid():
            raise Exception(f"Error when dry running SQL file {self.sqlfile}")

        if self.sqlfile in SKIP:
            print(f"\t...Ignoring dryrun results for {self.sqlfile}")

        if (
            self.dry_run_result
            and self.dry_run_result["valid"]
            and "referencedTables" in self.dry_run_result
        ):
            return self.dry_run_result["referencedTables"]

        # Handle views that require a date filter
        if (
            self.dry_run_result
            and self.strip_dml
            and self.get_error() == Errors.DATE_FILTER_NEEDED
        ):
            # Since different queries require different partition filters
            # (submission_date, crash_date, timestamp, submission_timestamp, ...)
            # We can extract the filter name from the error message
            # (by capturing the next word after "column(s)")

            # Example error:
            # "Cannot query over table <table_name> without a filter over column(s)
            # <date_filter_name> that can be used for partition elimination."

            error = self.dry_run_result["errors"][0].get("message", "")
            date_filter = find_next_word("column(s)", error)

            if "date" in date_filter:
                filtered_content = (
                    f"{self.get_sql()}WHERE {date_filter} > current_date()"
                )
                if (
                    DryRun(self.sqlfile, filtered_content).get_error()
                    == Errors.DATE_FILTER_NEEDED_AND_SYNTAX
                ):
                    # If the date filter (e.g. WHERE crash_date > current_date())
                    # is added to a query that already has a WHERE clause,
                    # it will throw an error. To fix this, we need to
                    # append 'AND' instead of 'WHERE'
                    filtered_content = (
                        f"{self.get_sql()}AND {date_filter} > current_date()"
                    )

            if "timestamp" in date_filter:
                filtered_content = (
                    f"{self.get_sql()}WHERE {date_filter} > current_timestamp()"
                )
                if (
                    DryRun(sqlfile=self.sqlfile, content=filtered_content).get_error()
                    == Errors.DATE_FILTER_NEEDED_AND_SYNTAX
                ):
                    filtered_content = (
                        f"{self.get_sql()}AND {date_filter} > current_timestamp()"
                    )

            stripped_dml_result = DryRun(sqlfile=self.sqlfile, content=filtered_content)
            if (
                stripped_dml_result.get_error() is None
                and "referencedTables" in stripped_dml_result.dry_run_result
            ):
                return stripped_dml_result.dry_run_result["referencedTables"]

        return []

    def get_schema(self):
        """Return the query schema by dry running the SQL file."""
        if self.sqlfile not in SKIP and not self.is_valid():
            raise Exception(f"Error when dry running SQL file {self.sqlfile}")

        if self.sqlfile in SKIP:
            print(f"\t...Ignoring dryrun results for {self.sqlfile}")
            return {}

        if (
            self.dry_run_result
            and self.dry_run_result["valid"]
            and "schema" in self.dry_run_result
        ):
            return self.dry_run_result["schema"]

        return {}

    def get_dataset_labels(self):
        """Return the labels on the default dataset by dry running the SQL file."""
        if self.sqlfile not in SKIP and not self.is_valid():
            raise Exception(f"Error when dry running SQL file {self.sqlfile}")

        if self.sqlfile in SKIP:
            print(f"\t...Ignoring dryrun results for {self.sqlfile}")
            return {}

        if (
            self.dry_run_result
            and self.dry_run_result["valid"]
            and "datasetLabels" in self.dry_run_result
        ):
            return self.dry_run_result["datasetLabels"]

        return {}

    def is_valid(self):
        """Dry run the provided SQL file and check if valid."""
        if self.dry_run_result is None:
            return False

        if self.dry_run_result["valid"]:
            print(f"{self.sqlfile:59} OK")
        elif self.get_error() == Errors.READ_ONLY:
            # We want the dryrun service to only have read permissions, so
            # we expect CREATE VIEW and CREATE TABLE to throw specific
            # exceptions.
            print(f"{self.sqlfile:59} OK")
        elif self.get_error() == Errors.DATE_FILTER_NEEDED and self.strip_dml:
            # With strip_dml flag, some queries require a partition filter
            # (submission_date, submission_timestamp, etc.) to run
            # We mark these requests as valid and add a date filter
            # in get_referenced_table()
            print(f"{self.sqlfile:59} OK but DATE FILTER NEEDED")
        else:
            print(f"{self.sqlfile:59} ERROR\n", self.dry_run_result["errors"])
            return False

        return True

    def errors(self):
        """Dry run the provided SQL file and return errors."""
        if self.dry_run_result is None:
            return []
        return self.dry_run_result.get("errors", [])

    def get_error(self):
        """Get specific errors for edge case handling."""
        errors = self.dry_run_result.get("errors", None)
        if errors and len(errors) == 1:
            error = errors[0]
        else:
            error = None
        if error and error.get("code", None) in [400, 403]:
            if (
                "does not have bigquery.tables.create permission for dataset"
                in error.get("message", "")
                or "Permission bigquery.tables.create denied"
                in error.get("message", "")
            ):
                return Errors.READ_ONLY
            if "without a filter over column(s)" in error.get("message", ""):
                return Errors.DATE_FILTER_NEEDED
            if (
                "Syntax error: Expected end of input but got keyword WHERE"
                in error.get("message", "")
            ):
                return Errors.DATE_FILTER_NEEDED_AND_SYNTAX
        return error


def sql_file_valid(sqlfile):
    """Dry run SQL files."""
    return DryRun(sqlfile).is_valid()


def find_next_word(target, source):
    """Find the next word in a string."""
    split = source.split()
    for i, w in enumerate(split):
        if w == target:
            # get the next word, and remove quotations from column name
            return split[i + 1].replace("'", "")


def main():
    """Dry run all SQL files in the project directories."""
    parser = ArgumentParser(description=main.__doc__)
    parser.add_argument(
        "paths",
        metavar="PATH",
        nargs="*",
        help="Paths to search for queries to dry run. CI passes 'sql' on the default "
        "branch, and the paths that have been modified since branching otherwise",
    )
    args = parser.parse_args()

    file_names = ("query.sql", "view.sql", "part*.sql", "init.sql")
    file_re = re.compile("|".join(map(fnmatch.translate, file_names)))

    sql_files: Set[str] = set()
    for path in args.paths:
        if isdir(path):
            sql_files |= {
                sql_file
                for pattern in file_names
                for sql_file in glob.glob(f"{path}/**/{pattern}", recursive=True)
            }
        elif file_re.fullmatch(basename(path)):
            sql_files.add(path)
    sql_files -= SKIP

    if not sql_files:
        print("Skipping dry run because no queries matched")
        sys.exit(0)

    with Pool(8) as p:
        result = p.map(sql_file_valid, sorted(sql_files), chunksize=1)
    if all(result):
        exitcode = 0
    else:
        exitcode = 1
    sys.exit(exitcode)


if __name__ == "__main__":
    main()
