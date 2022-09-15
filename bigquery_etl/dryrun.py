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

import glob
import json
import re
from enum import Enum
from os.path import basename, dirname, exists
from pathlib import Path
from urllib.request import Request, urlopen

import click
from google.cloud import bigquery

from .metadata.parse_metadata import Metadata

try:
    from functools import cached_property  # type: ignore
except ImportError:
    # python 3.7 compatibility
    from backports.cached_property import cached_property  # type: ignore


SKIP = {
    # Access Denied
    "sql/moz-fx-data-shared-prod/account_ecosystem_derived/ecosystem_client_id_lookup_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/account_ecosystem_derived/desktop_clients_daily_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/account_ecosystem_restricted/ecosystem_client_id_deletion_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/account_ecosystem_derived/fxa_logging_users_daily_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/activity_stream/impression_stats_flat/view.sql",
    "sql/moz-fx-data-shared-prod/activity_stream/tile_id_types/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/deletion_request_volume_v1/query.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/schema_error_counts_v1/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/schema_error_counts_v2/query.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/suggest_impression_rate_v1/init.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/suggest_impression_rate_v1/query.sql",
    "sql/moz-fx-data-shared-prod/monitoring/schema_error_counts_v1/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/structured_error_counts_v1/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring/structured_error_counts/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/telemetry_missing_columns_v1/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring/telemetry_missing_columns_v1/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/telemetry_missing_columns_v2/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring/telemetry_missing_columns_v2/view.sql",
    *glob.glob(
        "sql/moz-fx-data-shared-prod/monitoring*/suggest*_rate*_live*/*.sql",
        recursive=True,
    ),
    *glob.glob(
        "sql/moz-fx-data-shared-prod/monitoring*/topsites*_rate*_live*/*.sql",
        recursive=True,
    ),
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
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_amplitude_export_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_amplitude_user_ids_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_amplitude_user_ids_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/fxa_stdout_events_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/nonprod_fxa_auth_events_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/nonprod_fxa_content_events_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/nonprod_fxa_stdout_events_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/docker_fxa_admin_server_sanitized_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/docker_fxa_customs_sanitized_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/regrets_reporter/regrets_reporter_update/view.sql",
    "sql/moz-fx-data-shared-prod/revenue_derived/client_ltv_v1/query.sql",
    "sql/moz-fx-data-shared-prod/monitoring/payload_bytes_decoded_structured/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring/payload_bytes_decoded_stub_installer/view.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/monitoring/payload_bytes_decoded_telemetry/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring/payload_bytes_error_structured/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/shredder_progress/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring/shredder_progress/view.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/telemetry_distinct_docids_v1/query.sql",
    "sql/moz-fx-data-shared-prod/revenue_derived/client_ltv_normalized/query.sql",
    "sql/moz-fx-data-shared-prod/revenue_derived/client_ltv_normalized_v1/query.sql",
    *glob.glob("sql/moz-fx-data-shared-prod/stripe_derived/**/*.sql", recursive=True),
    *glob.glob("sql/moz-fx-data-shared-prod/stripe_external/**/*.sql", recursive=True),
    *glob.glob("sql/moz-fx-cjms-*/**/*.sql", recursive=True),
    "sql/moz-fx-data-shared-prod/subscription_platform/stripe_subscriptions/view.sql",
    "sql/moz-fx-data-shared-prod/subscription_platform/nonprod_stripe_subscriptions/view.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/stripe/itemized_payout_reconciliation/view.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/active_subscriptions_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/active_subscription_ids_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/add_device_events_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/all_subscriptions_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/channel_group_proportions_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/devices_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/fxa_attribution_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/funnel_product_page_to_subscribed_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/login_flows_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/protected_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/site_metrics_empty_check_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/site_metrics_summary_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/subscriptions_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/subscription_events_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/users_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/devices_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/subscriptions_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/users_v1/query.sql",
    "sql/moz-fx-data-shared-prod/monitoring_derived/telemetry_missing_columns_v3/query.sql",
    "sql/moz-fx-data-experiments/monitoring/query_cost_v1/query.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/subscriptions_v1/init.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/users_v1/init.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/protected_v1/init.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_derived/add_device_events_v1/init.sql",
    "sql/moz-fx-data-shared-prod/mozilla_vpn_external/devices_v1/init.sql",
    *glob.glob("sql/moz-fx-data-shared-prod/search_terms*/**/*.sql", recursive=True),
    "sql/moz-fx-data-bq-performance/release_criteria/dashboard_health_v1/query.sql",
    "sql/moz-fx-data-bq-performance/release_criteria/rc_flattened_test_data_v1/query.sql",
    "sql/moz-fx-data-bq-performance/release_criteria/release_criteria_summary_v1/query.sql",
    "sql/moz-fx-data-bq-performance/release_criteria/stale_tests_v1/query.sql",
    "sql/moz-fx-data-bq-performance/release_criteria/release_criteria_v1/query.sql",
    *glob.glob(
        "sql/moz-fx-data-shared-prod/contextual_services/**/*.sql", recursive=True
    ),
    *glob.glob(
        "sql/moz-fx-data-shared-prod/contextual_services_derived/**/*.sql",
        recursive=True,
    ),
    *glob.glob(
        "sql/moz-fx-data-shared-prod/**/topsites_impression/view.sql", recursive=True
    ),
    "sql/moz-fx-data-shared-prod/regrets_reporter/regrets_reporter_summary/view.sql",
    *glob.glob(
        "sql/moz-fx-data-shared-prod/regrets_reporter_derived/regrets_reporter_summary_v1/*.sql",  # noqa E501
        recursive=True,
    ),
    *glob.glob(
        "sql/moz-fx-data-shared-prod/mlhackweek_search/**/*.sql", recursive=True
    ),
    *glob.glob(
        "sql/moz-fx-data-shared-prod/regrets_reporter_ucs/**/*.sql", recursive=True
    ),
    "sql/moz-fx-data-shared-prod/telemetry/xfocsp_error_report/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/regrets_reporter_update/view.sql",
    *glob.glob(
        "sql/moz-fx-data-marketing-prod/acoustic/**/*.sql",
        recursive=True,
    ),
    # Materialized views
    "sql/moz-fx-data-shared-prod/telemetry_derived/experiment_search_events_live_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/experiment_events_live_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/org_mozilla_fenix_derived/experiment_search_events_live_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/org_mozilla_fenix_derived/experiment_events_live_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/org_mozilla_firefox_derived/experiment_search_events_live_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/org_mozilla_firefox_derived/experiment_events_live_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/org_mozilla_firefox_beta_derived/experiment_search_events_live_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/org_mozilla_firefox_beta_derived/experiment_events_live_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/experiment_enrollment_cumulative_population_estimate_v1/view.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry/experiment_enrollment_cumulative_population_estimate/view.sql",  # noqa E501
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
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_last_seen_v1/query.sql",  # noqa E501
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
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_histogram_aggregates_gpu_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_histogram_aggregates_parent_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_keyed_histogram_aggregates_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_histogram_aggregates_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_histogram_bucket_counts_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/glam_client_probe_counts_extract_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/scalar_percentiles_v1/query.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_scalar_probe_counts_v1/query.sql",  # noqa E501
    # Dataset sql/glam-fenix-dev:glam_etl was not found
    *glob.glob("sql/glam-fenix-dev/glam_etl/**/*.sql", recursive=True),
    # Query templates
    "sql/moz-fx-data-shared-prod/search_derived/mobile_search_clients_daily_v1/fenix_metrics.template.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/search_derived/mobile_search_clients_daily_v1/mobile_search_clients_daily.template.sql",  # noqa E501
    # Temporary table does not exist
    # TODO: remove this in a follow up PR
    *glob.glob(
        "sql/moz-fx-data-shared-prod/*/baseline_clients_first_seen_v1/*.sql",
        recursive=True,
    ),
    # Query too complex
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/event_types_history_v1/query.sql",
    "sql/moz-fx-data-shared-prod/firefox_accounts_derived/event_types_history_v1/init.sql",
    # Tests
    "sql/moz-fx-data-test-project/test/simple_view/view.sql",
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

    def __init__(
        self,
        sqlfile,
        content=None,
        strip_dml=False,
        use_cloud_function=True,
        client=None,
        respect_skip=True,
    ):
        """Instantiate DryRun class."""
        self.sqlfile = sqlfile
        self.content = content
        self.strip_dml = strip_dml
        self.use_cloud_function = use_cloud_function
        self.client = client if use_cloud_function or client else bigquery.Client()
        self.respect_skip = respect_skip
        try:
            self.metadata = Metadata.of_query_file(self.sqlfile)
        except FileNotFoundError:
            self.metadata = None

    def skip(self):
        """Determine if dry run should be skipped."""
        return self.respect_skip and self.sqlfile in SKIP

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
            if self.metadata:
                # use metadata to rewrite date-type query params as submission_date
                date_params = [
                    query_param
                    for query_param in (
                        self.metadata.scheduling.get("date_partition_parameter"),
                        *(
                            param.split(":", 1)[0]
                            for param in self.metadata.scheduling.get("parameters", [])
                            if re.fullmatch(r"[^:]+:DATE:{{\s*ds\s*}}", param)
                        ),
                    )
                    if query_param and query_param != "submission_date"
                ]
                if date_params:
                    pattern = re.compile(
                        "@("
                        + "|".join(date_params)
                        # match whole query parameter names
                        + ")(?![a-zA-Z0-9_])"
                    )
                    sql = pattern.sub("@submission_date", sql)
        dataset = basename(dirname(dirname(self.sqlfile)))
        try:
            if self.use_cloud_function:
                r = urlopen(
                    Request(
                        self.DRY_RUN_URL,
                        headers={"Content-Type": "application/json"},
                        data=json.dumps(
                            {
                                "dataset": dataset,
                                "query": sql,
                            }
                        ).encode("utf8"),
                        method="POST",
                    )
                )
                return json.load(r)
            else:
                project = basename(dirname(dirname(dirname(self.sqlfile))))
                job_config = bigquery.QueryJobConfig(
                    dry_run=True,
                    use_query_cache=False,
                    default_dataset=f"{project}.{dataset}",
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            "submission_date", "DATE", "2019-01-01"
                        )
                    ],
                )
                job = self.client.query(sql, job_config=job_config)
                try:
                    dataset_labels = self.client.get_dataset(job.default_dataset).labels
                except Exception as e:
                    # Most users do not have bigquery.datasets.get permission in
                    # moz-fx-data-shared-prod
                    # This should not prevent the dry run from running since the dataset
                    # labels are usually not required
                    if "Permission bigquery.datasets.get denied on dataset" in str(e):
                        dataset_labels = []
                    else:
                        raise e

                return {
                    "valid": True,
                    "referencedTables": [
                        ref.to_api_repr() for ref in job.referenced_tables
                    ],
                    "schema": (
                        job._properties.get("statistics", {})
                        .get("query", {})
                        .get("schema", {})
                    ),
                    "datasetLabels": dataset_labels,
                }
        except Exception as e:
            print(f"{self.sqlfile!s:59} ERROR\n", e)
            return None

    def get_referenced_tables(self):
        """Return referenced tables by dry running the SQL file."""
        if not self.skip() and not self.is_valid():
            raise Exception(f"Error when dry running SQL file {self.sqlfile}")

        if self.skip():
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
        if not self.skip() and not self.is_valid():
            raise Exception(f"Error when dry running SQL file {self.sqlfile}")

        if self.skip():
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
        if not self.skip() and not self.is_valid():
            raise Exception(f"Error when dry running SQL file {self.sqlfile}")

        if self.skip():
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
            print(f"{self.sqlfile!s:59} OK")
        elif self.get_error() == Errors.READ_ONLY:
            # We want the dryrun service to only have read permissions, so
            # we expect CREATE VIEW and CREATE TABLE to throw specific
            # exceptions.
            print(f"{self.sqlfile!s:59} OK")
        elif self.get_error() == Errors.DATE_FILTER_NEEDED and self.strip_dml:
            # With strip_dml flag, some queries require a partition filter
            # (submission_date, submission_timestamp, etc.) to run
            # We mark these requests as valid and add a date filter
            # in get_referenced_table()
            print(f"{self.sqlfile!s:59} OK but DATE FILTER NEEDED")
        else:
            print(f"{self.sqlfile!s:59} ERROR\n", self.dry_run_result["errors"])
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

    def validate_schema(self):
        """Check whether schema is valid."""
        # delay import to prevent circular imports in 'bigquery_etl.schema'
        from .schema import SCHEMA_FILE, Schema

        if self.skip() or basename(self.sqlfile) == "script.sql":
            print(f"\t...Ignoring schema validation for {self.sqlfile}")
            return True

        query_file_path = Path(self.sqlfile)
        query_schema = Schema.from_json(self.get_schema())
        if self.errors():
            # ignore file when there are errors that self.get_schema() did not raise
            click.echo(f"\t...Ignoring schema validation for {self.sqlfile}")
            return True
        existing_schema_path = query_file_path.parent / SCHEMA_FILE

        if not existing_schema_path.is_file():
            click.echo(f"No schema file defined for {query_file_path}", err=True)
            return True

        table_name = query_file_path.parent.name
        dataset_name = query_file_path.parent.parent.name
        project_name = query_file_path.parent.parent.parent.name

        partitioned_by = None
        if (
            self.metadata
            and self.metadata.bigquery
            and self.metadata.bigquery.time_partitioning
        ):
            partitioned_by = self.metadata.bigquery.time_partitioning.field

        table_schema = Schema.for_table(
            project_name, dataset_name, table_name, partitioned_by
        )

        if not query_schema.compatible(table_schema):
            click.echo(
                click.style(
                    f"ERROR: Schema for query in {query_file_path} "
                    f"incompatible with schema deployed for "
                    f"{project_name}.{dataset_name}.{table_name}",
                    fg="red",
                ),
                err=True,
            )
            return False
        else:
            existing_schema = Schema.from_schema_file(existing_schema_path)

            if not existing_schema.equal(query_schema):
                click.echo(
                    click.style(
                        f"Schema defined in {existing_schema_path} "
                        f"incompatible with query {query_file_path}",
                        fg="red",
                    ),
                    err=True,
                )
                return False

        click.echo(f"Schemas for {query_file_path} are valid.")
        return True


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
