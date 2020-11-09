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
from functools import cached_property
from multiprocessing.pool import ThreadPool
from os.path import basename, dirname
from urllib.request import urlopen, Request
import glob
import json
import sys

SKIP = {
    # Access Denied
    "sql/moz-fx-data-shared-prod/account_ecosystem_derived/ecosystem_client_id_lookup_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/account_ecosystem_derived/desktop_clients_daily_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/account_ecosystem_restricted/ecosystem_client_id_deletion_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/account_ecosystem_derived/fxa_logging_users_daily_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/activity_stream/impression_stats_flat/view.sql",
    "sql/moz-fx-data-shared-prod/activity_stream/tile_id_types/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring/deletion_request_volume_v1/query.sql",
    "sql/moz-fx-data-shared-prod/monitoring/document_sample_nonprod_v1/query.sql",
    "sql/moz-fx-data-shared-prod/monitoring/schema_error_counts_v1/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring/structured_error_counts_v1/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring/telemetry_missing_columns_v1/view.sql",
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
    "sql/moz-fx-data-shared-prod/telemetry_derived/addons_daily_v1/query.sql",
    "sql/moz-fx-data-shared-prod/search_derived/search_clients_last_seen_v1/init.sql",
    "sql/moz-fx-data-shared-prod/search_derived/search_clients_last_seen_v1/query.sql",
    "sql/moz-fx-data-shared-prod/search/search_rfm/view.sql",
    "sql/moz-fx-data-shared-prod/search/search_clients_last_seen_v1/view.sql",
    "sql/moz-fx-data-shared-prod/search/search_clients_last_seen/view.sql",
    "sql/moz-fx-data-shared-prod/firefox_accounts/fxa_amplitude_email_clicks/view.sql",
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_amplitude_export_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_amplitude_user_ids_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_amplitude_user_ids_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/regrets_reporter/regrets_reporter_update/view.sql",
    "sql/moz-fx-data-shared-prod/revenue_derived/client_ltv_v1/query.sql",
    "sql/moz-fx-data-shared-prod/shredder_state/progress/view.sql",
    "sql/moz-fx-data-shared-prod/apple_app_store/metrics_by_app_referrer/query.sql",
    "sql/moz-fx-data-shared-prod/apple_app_store/metrics_by_app_version/query.sql",
    "sql/moz-fx-data-shared-prod/apple_app_store/metrics_by_campaign/query.sql",
    "sql/moz-fx-data-shared-prod/apple_app_store/metrics_by_platform/query.sql",
    "sql/moz-fx-data-shared-prod/apple_app_store/metrics_by_platform_version/query.sql",
    "sql/moz-fx-data-shared-prod/apple_app_store/metrics_by_region/query.sql",
    "sql/moz-fx-data-shared-prod/apple_app_store/metrics_by_source/query.sql",
    "sql/moz-fx-data-shared-prod/apple_app_store/metrics_by_storefront/query.sql",
    "sql/moz-fx-data-shared-prod/apple_app_store/metrics_by_web_referrer/query.sql",
    "sql/moz-fx-data-shared-prod/apple_app_store/metrics_total/query.sql",
    "sql/moz-fx-data-shared-prod/monitoring/telemetry_distinct_docids_v1/query.sql",
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
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/login_flows_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/protected_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/users_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/waitlist_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/devices_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/subscriptions_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/users_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/waitlist_v1/query.sql",
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
    "sql/moz-fx-data-shared-prod/monitoring/structured_detailed_error_counts_v1/view.sql",  # noqa E501
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
    "sql/moz-fx-data-shared-prod/telemetry_derived/asn_aggregates_v1/query.sql",
    # Dataset sql/glam-fenix-dev:glam_etl was not found
    *glob.glob("sql/glam-fenix-dev/glam_etl/**/*.sql", recursive=True),
    # Query templates
    "sql/moz-fx-data-shared-prod/search_derived/mobile_search_clients_daily_v1/fenix_metrics.template.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/search_derived/mobile_search_clients_daily_v1/mobile_search_clients_daily.template.sql",  # noqa
}


class DryRun:
    """Dry run SQL files."""

    # todo: support different dry run endpoints for different projects
    DRY_RUN_URL = (
        "https://us-central1-moz-fx-data-shared-prod.cloudfunctions.net/"
        "bigquery-etl-dryrun"
    )

    def __init__(self, sqlfile):
        """Instantiate DryRun class."""
        self.sqlfile = sqlfile

    @cached_property
    def dry_run_result(self):
        """Dry run the provided SQL file."""
        sql = open(self.sqlfile).read()

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
        if not self.is_valid():
            raise Exception(f"Error when dry running SQL file {self.sqlfile}")

        if (
            self.dry_run_result
            and self.dry_run_result["valid"]
            and "referencedTables" in self.dry_run_result
        ):
            return self.dry_run_result["referencedTables"]

        return []

    def is_valid(self):
        """Dry run the provided SQL file and check if valid."""
        if self.dry_run_result is None:
            return False

        if "errors" in self.dry_run_result and len(self.dry_run_result["errors"]) == 1:
            error = self.dry_run_result["errors"][0]
        else:
            error = None

        if self.dry_run_result["valid"]:
            print(f"{self.sqlfile:59} OK")
        elif (
            error
            and error.get("code", None) in [400, 403]
            and "does not have bigquery.tables.create permission for dataset"
            in error.get("message", "")
        ):
            # We want the dryrun service to only have read permissions, so
            # we expect CREATE VIEW and CREATE TABLE to throw specific
            # exceptions.
            print(f"{self.sqlfile:59} OK")
        else:
            print(f"{self.sqlfile:59} ERROR\n", self.dry_run_result["errors"])
            return False

        return True


def main():
    """Dry run all SQL files in the project directories."""
    file_names = ("query.sql", "view.sql", "part*.sql")

    sql_files = [
        f
        for n in file_names
        for f in glob.glob(f"sql/**/{n}", recursive=True)
        if f not in SKIP
    ]

    def sql_file_valid(sqlfile):
        """Dry run SQL files."""
        return DryRun(sqlfile).is_valid()

    with ThreadPool(8) as p:
        result = p.map(sql_file_valid, sql_files, chunksize=1)
    if all(result):
        exitcode = 0
    else:
        exitcode = 1
    sys.exit(exitcode)


if __name__ == "__main__":
    main()
