"""
Dry run query files.

Passes all queries defined under sql/ to a Cloud Function that will run the
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
    "sql/activity_stream/impression_stats_flat/view.sql",
    "sql/activity_stream/tile_id_types/view.sql",
    "sql/monitoring/deletion_request_volume_v1/query.sql",
    "sql/monitoring/document_sample_nonprod_v1/query.sql",
    "sql/monitoring/schema_error_counts_v1/view.sql",
    "sql/monitoring/structured_error_counts_v1/view.sql",
    "sql/pocket/pocket_reach_mau/view.sql",
    "sql/telemetry/buildhub2/view.sql",
    "sql/firefox_accounts_derived/fxa_content_events_v1/query.sql",
    "sql/firefox_accounts_derived/fxa_auth_bounce_events_v1/query.sql",
    "sql/firefox_accounts_derived/fxa_auth_events_v1/query.sql",
    "sql/firefox_accounts_derived/fxa_delete_events_v1/init.sql",
    "sql/firefox_accounts_derived/fxa_delete_events_v1/query.sql",
    "sql/firefox_accounts_derived/fxa_oauth_events_v1/query.sql",
    "sql/firefox_accounts_derived/fxa_log_auth_events_v1/query.sql",
    "sql/firefox_accounts_derived/fxa_log_content_events_v1/query.sql",
    "sql/firefox_accounts_derived/fxa_log_device_command_events_v1/query.sql",
    "sql/telemetry_derived/addons_daily_v1/query.sql",
    "sql/search_derived/search_clients_last_seen_v1/init.sql",
    "sql/search_derived/search_clients_last_seen_v1/query.sql",
    "sql/search/search_rfm/view.sql",
    "sql/search/search_clients_last_seen_v1/view.sql",
    "sql/search/search_clients_last_seen/view.sql",
    "sql/firefox_accounts/fxa_amplitude_email_clicks/view.sql",
    "sql/firefox_accounts_derived/fxa_amplitude_export_v1/query.sql",
    "sql/firefox_accounts_derived/fxa_amplitude_user_ids_v1/query.sql",
    "sql/firefox_accounts_derived/fxa_amplitude_user_ids_v1/init.sql",
    "sql/revenue_derived/client_ltv_v1/query.sql",
    "sql/shredder_state/progress/view.sql",
    "sql/apple_app_store/metrics_by_app_referrer/query.sql",
    "sql/apple_app_store/metrics_by_app_version/query.sql",
    "sql/apple_app_store/metrics_by_campaign/query.sql",
    "sql/apple_app_store/metrics_by_platform/query.sql",
    "sql/apple_app_store/metrics_by_platform_version/query.sql",
    "sql/apple_app_store/metrics_by_region/query.sql",
    "sql/apple_app_store/metrics_by_source/query.sql",
    "sql/apple_app_store/metrics_by_storefront/query.sql",
    "sql/apple_app_store/metrics_by_web_referrer/query.sql",
    "sql/apple_app_store/metrics_total/query.sql",
    "sql/monitoring/telemetry_distinct_docids_v1/query.sql",
    # Already exists (and lacks an "OR REPLACE" clause)
    "sql/org_mozilla_firefox_derived/clients_first_seen_v1/init.sql",
    "sql/org_mozilla_firefox_derived/clients_last_seen_v1/init.sql",
    "sql/org_mozilla_fenix_derived/clients_last_seen_v1/init.sql",
    "sql/org_mozilla_vrbrowser_derived/clients_last_seen_v1/init.sql",
    "sql/telemetry_derived/core_clients_last_seen_v1/init.sql",
    "sql/telemetry/fxa_users_last_seen_raw_v1/init.sql",
    "sql/telemetry_derived/core_clients_first_seen_v1/init.sql",
    "sql/telemetry_derived/fxa_users_services_last_seen_v1/init.sql",
    "sql/messaging_system_derived/cfr_users_last_seen_v1/init.sql",
    "sql/messaging_system_derived/onboarding_users_last_seen_v1/init.sql",
    "sql/messaging_system_derived/snippets_users_last_seen_v1/init.sql",
    "sql/messaging_system_derived/whats_new_panel_users_last_seen_v1/init.sql",
    # Reference table not found
    "sql/monitoring/structured_detailed_error_counts_v1/view.sql",
    "sql/org_mozilla_firefox_derived/migrated_clients_v1/query.sql",
    "sql/org_mozilla_firefox_derived/incline_executive_v1/query.sql",
    "sql/org_mozilla_firefox/migrated_clients/view.sql",
    # No matching signature for function IF
    "sql/static/fxa_amplitude_export_users_last_seen/query.sql",
    # Duplicate UDF
    "sql/static/fxa_amplitude_export_users_daily/query.sql",
    # Syntax error
    "sql/telemetry_derived/clients_last_seen_v1/init.sql",
    # HTTP Error 408: Request Time-out
    "sql/telemetry_derived/latest_versions/query.sql",
    "sql/telemetry_derived/italy_covid19_outage_v1/query.sql",
    # Query parameter not found
    "sql/telemetry_derived/experiments_v1/query.sql",
    "sql/telemetry_derived/clients_daily_scalar_aggregates_v1/query.sql",
    "sql/telemetry_derived/clients_daily_keyed_scalar_aggregates_v1/query.sql",
    "sql/telemetry_derived/clients_daily_keyed_boolean_aggregates_v1/query.sql",
    "sql/telemetry_derived/clients_daily_histogram_aggregates_content_v1/query.sql",
    "sql/telemetry_derived/clients_daily_histogram_aggregates_parent_v1/query.sql",
    "sql/telemetry_derived/clients_daily_keyed_histogram_aggregates_v1/query.sql",
    "sql/telemetry_derived/clients_histogram_aggregates_v1/query.sql",
    "sql/telemetry_derived/clients_histogram_bucket_counts_v1/query.sql",
    "sql/telemetry_derived/glam_client_probe_counts_extract_v1/query.sql",
    "sql/telemetry_derived/asn_aggregates_v1/query.sql",
    # Dataset moz-fx-data-shared-prod:glam_etl was not found
    *glob.glob("sql/glam_etl/**/*.sql", recursive=True),
    # Query templates
    "sql/search_derived/mobile_search_clients_daily_v1/fenix_metrics.template.sql",
    "sql/search_derived/mobile_search_clients_daily_v1/mobile_search_clients_daily.template.sql",  # noqa
}


class DryRun:
    """Dry run SQL files."""

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
    """Dry run all SQL files in the sql/ directory."""
    sql_files = [f for f in glob.glob("sql/**/*.sql", recursive=True) if f not in SKIP]

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
