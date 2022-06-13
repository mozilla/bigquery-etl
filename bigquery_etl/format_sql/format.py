"""Format SQL."""

import glob
import os
import os.path
import sys

from bigquery_etl.format_sql.formatter import reformat  # noqa E402

SKIP = {
    # files that existed before we started to enforce this script
    *glob.glob("bigquery_etl/glam/templates/*.sql"),
    *glob.glob("sql_generators/events_daily/templates/**/*.sql"),
    *glob.glob("sql_generators/glean_usage/templates/*.sql"),
    *glob.glob("sql_generators/search/templates/*.sql"),
    *glob.glob("sql_generators/experiment_monitoring/templates/**/*.sql"),
    *glob.glob("sql_generators/feature_usage/templates/*.sql"),
    "sql/moz-fx-data-shared-prod/telemetry/fenix_events_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/fennec_ios_events_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/fire_tv_events_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/first_shutdown_summary/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/first_shutdown_summary_v4/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/focus_android_events_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/lockwise_mobile_events_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/origin_content_blocking/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/rocket_android_events_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/simpleprophet_forecasts/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/smoot_usage_day_0/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/smoot_usage_day_13/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/sync/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_anonymous_parquet/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_anonymous_parquet_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_core_parquet/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_core_parquet_v3/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_downgrade_parquet/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_downgrade_parquet_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_focus_event_parquet/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_focus_event_parquet_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_heartbeat_parquet/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_heartbeat_parquet_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_ip_privacy_parquet/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_ip_privacy_parquet_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_mobile_event_parquet/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_mobile_event_parquet_v2/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_new_profile_parquet/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_new_profile_parquet_v2/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_shield_study_parquet/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/telemetry_shield_study_parquet_v1/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/windows_10_aggregate/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/windows_10_build_distribution/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry/windows_10_patch_adoption/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_histogram_aggregates_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_histogram_aggregates_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_keyed_boolean_aggregates_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_keyed_histogram_aggregates_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_keyed_scalar_aggregates_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_scalar_aggregates_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_scalar_aggregates_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_histogram_aggregates_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_histogram_aggregates_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_histogram_probe_counts_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/clients_scalar_probe_counts_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/core_clients_daily_v1/query.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/core_clients_last_seen_v1/init.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/core_clients_last_seen_v1/query.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/core_live/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/devtools_events_amplitude_v1/view.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/error_aggregates/query.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/event_events_v1/init.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/event_events_v1/query.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/events_live/view.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/experiment_enrollment_aggregates_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/experiment_enrollment_aggregates_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/experiments_daily_active_clients_v1/init.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/firefox_desktop_exact_mau28_by_client_count_dimensions_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/fxa_users_services_daily_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/fxa_users_services_last_seen_v1/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/telemetry_derived/glam_user_counts_v1/query.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/latest_versions/query.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/main_events_v1/init.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/main_events_v1/query.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/scalar_percentiles_v1/query.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/smoot_usage_desktop_v2/query.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/smoot_usage_fxa_v2/query.sql",
    "sql/moz-fx-data-shared-prod/telemetry_derived/smoot_usage_new_profiles_v2/query.sql",  # noqa E501
    "sql/moz-fx-data-shared-prod/udf_js/jackknife_percentile_ci/udf.sql",
    "sql/moz-fx-data-shared-prod/udf_legacy/contains.sql",
    "sql/moz-fx-data-shared-prod/udf_legacy/date_format.sql",
    "sql/moz-fx-data-shared-prod/udf_legacy/date_trunc.sql",
    "sql/moz-fx-data-shared-prod/udf_legacy/to_iso8601.sql",
    "stored_procedures/safe_crc32_uuid.sql",
}


def format(paths, check=False):
    """Format SQL files."""
    if not paths:
        query = sys.stdin.read()
        formatted = reformat(query, trailing_newline=True)
        if not check:
            print(formatted, end="")
        if check and query != formatted:
            sys.exit(1)
    else:
        sql_files = []
        for path in paths:
            if os.path.isdir(path):
                sql_files.extend(
                    filepath
                    for dirpath, _, filenames in os.walk(path)
                    for filename in filenames
                    if filename.endswith(".sql")
                    # skip tests/**/input.sql
                    and not (path.startswith("tests") and filename == "input.sql")
                    for filepath in [os.path.join(dirpath, filename)]
                    if filepath not in SKIP
                )
            elif path:
                sql_files.append(path)
        if not sql_files:
            print("Error: no files were found to format")
            sys.exit(255)
        sql_files.sort()
        reformatted = unchanged = 0
        for path in sql_files:
            with open(path) as fp:
                query = fp.read()
            formatted = reformat(query, trailing_newline=True)
            if query != formatted:
                if check:
                    print(f"would reformat {path}")
                else:
                    with open(path, "w") as fp:
                        fp.write(formatted)
                    print(f"reformatted {path}")
                reformatted += 1
            else:
                unchanged += 1
        print(
            ", ".join(
                f"{number} file{'s' if number > 1 else ''}"
                f"{' would be' if check else ''} {msg}"
                for number, msg in [
                    (reformatted, "reformatted"),
                    (unchanged, "left unchanged"),
                ]
                if number > 0
            )
            + "."
        )
        if check and reformatted:
            sys.exit(1)
