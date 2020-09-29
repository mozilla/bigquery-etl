"""Format SQL."""

from argparse import ArgumentParser
import glob
import os
import os.path
import sys

from bigquery_etl.format_sql.formatter import reformat  # noqa E402


SKIP = {
    # files that existed before we started to enforce this script
    *glob.glob("bigquery_etl/glam/templates/*.sql"),
    "bigquery_etl/glean_usage/templates/baseline_clients_daily.view.sql",
    "bigquery_etl/glean_usage/templates/baseline_clients_daily_v1.sql",
    "bigquery_etl/glean_usage/templates/baseline_clients_last_seen.view.sql",
    "bigquery_etl/glean_usage/templates/baseline_clients_last_seen_v1.sql",
    "moz-fx-data-shared-prod/sql/telemetry/experiments_aggregates_v1/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/fenix_events_v1/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/fennec_ios_events_v1/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/fire_tv_events_v1/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/first_shutdown_summary/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/first_shutdown_summary_v4/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/focus_android_events_v1/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/lockwise_mobile_events_v1/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/origin_content_blocking/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/rocket_android_events_v1/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/simpleprophet_forecasts/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/smoot_usage_day_0/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/smoot_usage_day_13/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/sync/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_anonymous_parquet/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_anonymous_parquet_v1/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_core_parquet/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_core_parquet_v3/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_downgrade_parquet/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_downgrade_parquet_v1/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_focus_event_parquet/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_focus_event_parquet_v1/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_heartbeat_parquet/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_heartbeat_parquet_v1/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_ip_privacy_parquet/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_ip_privacy_parquet_v1/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_mobile_event_parquet/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_mobile_event_parquet_v2/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_new_profile_parquet/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_new_profile_parquet_v2/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_shield_study_parquet/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/telemetry_shield_study_parquet_v1/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/windows_10_aggregate/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/windows_10_build_distribution/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry/windows_10_patch_adoption/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/attitudes_daily_v1/init.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/attitudes_daily_v1/query.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/clients_daily_histogram_aggregates_v1/init.sql",   # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/clients_daily_histogram_aggregates_v1/query.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/clients_daily_keyed_boolean_aggregates_v1/query.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/clients_daily_keyed_histogram_aggregates_v1/query.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/clients_daily_keyed_scalar_aggregates_v1/query.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/clients_daily_scalar_aggregates_v1/init.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/clients_daily_scalar_aggregates_v1/query.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/clients_histogram_aggregates_v1/init.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/clients_histogram_aggregates_v1/query.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/clients_histogram_probe_counts_v1/query.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/clients_scalar_probe_counts_v1/query.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/core_clients_daily_v1/query.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/core_clients_last_seen_v1/init.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/core_clients_last_seen_v1/query.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/core_live/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/devtools_events_amplitude_v1/view.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/error_aggregates/query.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/event_events_v1/init.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/event_events_v1/query.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/events_live/view.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/experiment_enrollment_aggregates_v1/init.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/experiment_enrollment_aggregates_v1/query.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/experiments_daily_active_clients_v1/init.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/firefox_desktop_exact_mau28_by_client_count_dimensions_v1/query.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/fxa_users_services_daily_v1/query.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/fxa_users_services_last_seen_v1/query.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/glam_user_counts_v1/query.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/latest_versions/query.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/main_events_v1/init.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/main_events_v1/query.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/scalar_percentiles_v1/query.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/smoot_usage_desktop_v2/query.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/smoot_usage_fxa_v2/query.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/smoot_usage_new_profiles_v2/query.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/telemetry_derived/smoot_usage_nondesktop_v2/query.sql",
    "moz-fx-data-shared-prod/sql/telemetry_derived/surveygizmo_daily_attitudes/init.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/search_derived/mobile_search_clients_daily_v1/fenix_metrics.template.sql",  # noqa E501
    "moz-fx-data-shared-prod/sql/search_derived/mobile_search_clients_daily_v1/mobile_search_clients_daily.template.sql",  # noqa E501
    "moz-fx-data-shared-prod/udf/active_n_weeks_ago.sql",
    "moz-fx-data-shared-prod/udf/add_monthly_engine_searches.sql",
    "moz-fx-data-shared-prod/udf/add_monthly_searches.sql",
    "moz-fx-data-shared-prod/udf/aggregate_search_map.sql",
    "moz-fx-data-shared-prod/udf/array_11_zeroes_then.sql",
    "moz-fx-data-shared-prod/udf/array_drop_first_and_append.sql",
    "moz-fx-data-shared-prod/udf/array_of_12_zeroes.sql",
    "moz-fx-data-shared-prod/udf/bitcount_lowest_7.sql",
    "moz-fx-data-shared-prod/udf/bitmask_365.sql",
    "moz-fx-data-shared-prod/udf/bitmask_lowest_28.sql",
    "moz-fx-data-shared-prod/udf/bitmask_lowest_7.sql",
    "moz-fx-data-shared-prod/udf/bitmask_range.sql",
    "moz-fx-data-shared-prod/udf/bits_to_days_seen.sql",
    "moz-fx-data-shared-prod/udf/bits_to_days_since_first_seen.sql",
    "moz-fx-data-shared-prod/udf/bits_to_days_since_seen.sql",
    "moz-fx-data-shared-prod/udf/bool_to_365_bits.sql",
    "moz-fx-data-shared-prod/udf/coalesce_adjacent_days_28_bits.sql",
    "moz-fx-data-shared-prod/udf/coalesce_adjacent_days_365_bits.sql",
    "moz-fx-data-shared-prod/udf/combine_adjacent_days_365_bits.sql",
    "moz-fx-data-shared-prod/udf/combine_experiment_days.sql",
    "moz-fx-data-shared-prod/udf/country_code_to_flag.sql",
    "moz-fx-data-shared-prod/udf/days_seen_bytes_to_rfm.sql",
    "moz-fx-data-shared-prod/udf/days_since_created_profile_as_28_bits.sql",
    "moz-fx-data-shared-prod/udf/deanonymize_event.sql",
    "moz-fx-data-shared-prod/udf/decode_int64.sql",
    "moz-fx-data-shared-prod/udf/dedupe_array.sql",
    "moz-fx-data-shared-prod/udf/extract_count_histogram_value.sql",
    "moz-fx-data-shared-prod/udf/extract_document_type.sql",
    "moz-fx-data-shared-prod/udf/extract_document_version.sql",
    "moz-fx-data-shared-prod/udf/glean_timespan_nanos.sql",
    "moz-fx-data-shared-prod/udf/glean_timespan_seconds.sql",
    "moz-fx-data-shared-prod/udf/int_to_365_bits.sql",
    "moz-fx-data-shared-prod/udf/int_to_hex_string.sql",
    "moz-fx-data-shared-prod/udf/keyed_histogram_get_sum.sql",
    "moz-fx-data-shared-prod/udf/kv_array_to_json_string.sql",
    "moz-fx-data-shared-prod/udf/mode_last.sql",
    "moz-fx-data-shared-prod/udf/new_monthly_engine_searches_struct.sql",
    "moz-fx-data-shared-prod/udf/normalize_glean_ping_info.sql",
    "moz-fx-data-shared-prod/udf/normalize_metadata.sql",
    "moz-fx-data-shared-prod/udf/normalize_search_engine.sql",
    "moz-fx-data-shared-prod/udf/one_as_365_bits.sql",
    "moz-fx-data-shared-prod/udf/parse_desktop_telemetry_uri.sql",
    "moz-fx-data-shared-prod/udf/parse_iso8601_date.sql",
    "moz-fx-data-shared-prod/udf/pos_of_leading_set_bit.sql",
    "moz-fx-data-shared-prod/udf/pos_of_trailing_set_bit.sql",
    "moz-fx-data-shared-prod/udf/round_timestamp_to_minute.sql",
    "moz-fx-data-shared-prod/udf/shift_28_bits_one_day.sql",
    "moz-fx-data-shared-prod/udf/shift_365_bits_one_day.sql",
    "moz-fx-data-shared-prod/udf/smoot_usage_from_28_bits.sql",
    "moz-fx-data-shared-prod/udf/vector_add.sql",
    "moz-fx-data-shared-prod/udf/zero_as_365_bits.sql",
    "moz-fx-data-shared-prod/udf/zeroed_array.sql",
    "moz-fx-data-shared-prod/udf_js/crc32.sql",
    "moz-fx-data-shared-prod/udf_js/gunzip.sql",
    "moz-fx-data-shared-prod/udf_js/jackknife_mean_ci.sql",
    "moz-fx-data-shared-prod/udf_js/jackknife_ratio_ci.sql",
    "moz-fx-data-shared-prod/udf_js/jackknife_sum_ci.sql",
    "moz-fx-data-shared-prod/udf_js/json_extract_events.sql",
    "moz-fx-data-shared-prod/udf_js/json_extract_histogram.sql",
    "moz-fx-data-shared-prod/udf_js/json_extract_keyed_histogram.sql",
    "moz-fx-data-shared-prod/udf_js/json_extract_missing_cols.sql",
    "moz-fx-data-shared-prod/udf_js/sample_id.sql",
    "moz-fx-data-shared-prod/udf_legacy/contains.sql",
    "moz-fx-data-shared-prod/udf_legacy/date_format.sql",
    "moz-fx-data-shared-prod/udf_legacy/date_trunc.sql",
    "moz-fx-data-shared-prod/udf_legacy/to_iso8601.sql",
    "stored_procedures/safe_crc32_uuid.sql",
}

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "paths",
    metavar="PATH",
    nargs="*",
    help="file or directory to format;"
    " if not specified read from stdin and write to stdout;"
    " recursively search directories for .sql files",
)
parser.add_argument(
    "--check",
    action="store_true",
    help="do not write changes, just return status;"
    " return code 0 indicates nothing would change;"
    " return code 1 indicates some files would be reformatted",
)


def main():
    """Format SQL files."""
    args = parser.parse_args()
    format(args.paths, args.check)


def format(paths, check=False):
    """Format SQL files."""
    if not paths:
        if sys.stdin.isatty():
            parser.print_help()
            print("Error: must specify PATH or provide input via stdin")
            sys.exit(255)
        query = sys.stdin.read()
        formatted = reformat(query) + "\n"
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
            formatted = reformat(query) + "\n"
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


if __name__ == "__main__":
    main()
