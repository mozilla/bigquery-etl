# Load libraries
import json
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from argparse import ArgumentParser
from google.cloud import bigquery
from google.cloud import storage

# Configs
device_usg_configs = {
    "timeout_limit": 500,
    "locations": [
        "ALL",
        "AU",
        "BE",
        "BG",
        "BR",
        "CA",
        "CH",
        "CZ",
        "DE",
        "DK",
        "EE",
        "ES",
        "FI",
        "FR",
        "GB",
        "HR",
        "IE",
        "IN",
        "IT",
        "JP",
        "KE",
        "CY",
        "LV",
        "LT",
        "LU",
        "HU",
        "MT",
        "MX",
        "NL",
        "AT",
        "PL",
        "PT",
        "RO",
        "SI",
        "SK",
        "US",
        "SE",
        "GR",
    ],
    "bucket": "gs://moz-fx-data-prod-external-data/",
    "bucket_no_gs": "moz-fx-data-prod-external-data",
    "results_stg_gcs_fpth": "cloudflare/device_usage/RESULTS_STAGING/%s_results_from_%s_run.csv",
    "results_archive_gcs_fpath": "cloudflare/device_usage/RESULTS_ARCHIVE/%s_results_from_%s_run.csv",
    "errors_stg_gcs_fpth": "cloudflare/device_usage/ERRORS_STAGING/%s_errors_from_%s_run.csv",
    "errors_archive_gcs_fpath": "cloudflare/device_usage/ERRORS_ARCHIVE/%s_errors_from_%s_run.csv",
    "gcp_project_id": "moz-fx-data-shared-prod",
    "gcp_conn_id": "google_cloud_shared_prod",
    "results_bq_stg_table": "moz-fx-data-shared-prod.cloudflare_derived.device_results_stg",
    "errors_bq_stg_table": "moz-fx-data-shared-prod.cloudflare_derived.device_errors_stg",
}

#Load the Cloudflare API Token
cloudflare_api_token = os.getenv("CLOUDFLARE_AUTH_TOKEN")

# Define a function to move a GCS object then delete the original
def move_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name):
    """Moves a blob from one bucket to another with a new name."""
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)
    destination_generation_match_precondition = None

    blob_copy = source_bucket.copy_blob(
        source_blob,
        destination_bucket,
        destination_blob_name,
        if_generation_match=destination_generation_match_precondition,
    )
    source_bucket.delete_blob(blob_name)

    print(
        "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )


def generate_device_type_timeseries_api_call(strt_dt, end_dt, agg_int, location):
    """Calculate API to call based on given parameters."""
    if location == "ALL":
        device_usage_api_url = f"https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/device_type?name=human&botClass=LIKELY_HUMAN&dateStart={strt_dt}T00:00:00.000Z&dateEnd={end_dt}T00:00:00.000Z&name=bot&botClass=LIKELY_AUTOMATED&dateStart={strt_dt}T00:00:00.000Z&dateEnd={end_dt}T00:00:00.000Z&format=json&aggInterval={agg_int}"
    else:
        device_usage_api_url = f"https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/device_type?name=human&botClass=LIKELY_HUMAN&dateStart={strt_dt}T00:00:00.000Z&dateEnd={end_dt}T00:00:00.000Z&location={location}&name=bot&botClass=LIKELY_AUTOMATED&dateStart={strt_dt}T00:00:00.000Z&dateEnd={end_dt}T00:00:00.000Z&location={location}&format=json&aggInterval={agg_int}"
    return device_usage_api_url


def parse_device_type_timeseries_response_human(result):
    """Take the response JSON and returns parsed human traffic information."""
    human_timestamps = result["human"]["timestamps"][0]
    human_desktop = result["human"]["desktop"][0]
    human_mobile = result["human"]["mobile"][0]
    human_other = result["human"]["other"][0]
    return human_timestamps, human_desktop, human_mobile, human_other


def parse_device_type_timeseries_response_bot(result):
    """Take the response JSON and returns parsed bot traffic information."""
    bot_timestamps = result["bot"]["timestamps"][0]
    bot_desktop = result["bot"]["desktop"][0]
    bot_mobile = result["bot"]["mobile"][0]
    bot_other = result["bot"]["other"][0]
    return bot_timestamps, bot_desktop, bot_mobile, bot_other


# Generate the result dataframe
def make_device_usage_result_df(
    user_type,
    desktop,
    mobile,
    other,
    timestamps,
    last_upd,
    norm,
    conf,
    agg_interval,
    location,
):
    """Initialize a result dataframe for device usage data."""
    return pd.DataFrame(
        {
            "Timestamp": timestamps,
            "UserType": [user_type] * len(timestamps),
            "Location": [location] * len(timestamps),
            "DesktopUsagePct": desktop,
            "MobileUsagePct": mobile,
            "OtherUsagePct": other,
            "ConfLevel": [conf] * len(timestamps),
            "AggInterval": [agg_interval] * len(timestamps),
            "NormalizationType": [norm] * len(timestamps),
            "LastUpdated": [last_upd] * len(timestamps),
        }
    )


def get_device_usage_data(date_of_interest, auth_token):
    """Call API and retrieve device usage data and save both errors & results to GCS."""
    # Calculate start date and end date
    logical_dag_dt = date_of_interest
    logical_dag_dt_as_date = datetime.strptime(logical_dag_dt, "%Y-%m-%d").date()
    start_date = logical_dag_dt_as_date - timedelta(days=4)
    end_date = start_date + timedelta(days=1)
    print("Start Date: ", start_date)
    print("End Date: ", end_date)

    # Configure request headers
    bearer_string = f"Bearer {auth_token}"
    headers = {"Authorization": bearer_string}

    # Initialize the empty results & errors dataframe
    results_df = pd.DataFrame(
        {
            "Timestamp": [],
            "UserType": [],
            "Location": [],
            "DesktopUsagePct": [],
            "MobileUsagePct": [],
            "OtherUsagePct": [],
            "ConfLevel": [],
            "AggInterval": [],
            "NormalizationType": [],
            "LastUpdated": [],
        }
    )

    errors_df = pd.DataFrame({"StartTime": [], "EndTime": [], "Location": []})

    # For each location, call the API to get device usage data
    for loc in device_usg_configs["locations"]:
        print("Loc: ", loc)

        # Generate the URL
        device_usage_api_url = generate_device_type_timeseries_api_call(
            start_date, end_date, "1d", loc
        )
        try:
            # Call the API and save the response as JSON
            response = requests.get(
                device_usage_api_url,
                headers=headers,
                timeout=device_usg_configs["timeout_limit"],
            )
            response_json = json.loads(response.text)
            # If response was successful, get the result
            if response_json["success"] is True:

                result = response_json["result"]
                human_ts, human_dsktp, human_mbl, human_othr = (
                    parse_device_type_timeseries_response_human(result)
                )
                bot_ts, bot_dsktp, bot_mbl, bot_othr = (
                    parse_device_type_timeseries_response_bot(result)
                )
                conf_lvl = result["meta"]["confidenceInfo"]["level"]
                aggr_intvl = result["meta"]["aggInterval"]
                nrmlztn = result["meta"]["normalization"]
                lst_upd = result["meta"]["lastUpdated"]

                # Save to the results dataframe ### FIX BELOW HERE ####
                human_result_df = make_device_usage_result_df(
                    "Human",
                    human_dsktp,
                    human_mbl,
                    human_othr,
                    human_ts,
                    lst_upd,
                    nrmlztn,
                    conf_lvl,
                    aggr_intvl,
                    loc,
                )

                bot_result_df = make_device_usage_result_df(
                    "Bot",
                    bot_dsktp,
                    bot_mbl,
                    bot_othr,
                    bot_ts,
                    lst_upd,
                    nrmlztn,
                    conf_lvl,
                    aggr_intvl,
                    loc,
                )

                # Union the results
                new_result_df = pd.concat(
                    [human_result_df, bot_result_df], ignore_index=True, sort=False
                )

                # Add results to the results dataframe
                results_df = pd.concat([results_df, new_result_df])

            # If response was not successful, save to the errors dataframe
            else:
                new_errors_df = pd.DataFrame(
                    {
                        "StartTime": [start_date],
                        "EndTime": [end_date],
                        "Location": [loc],
                    }
                )
                errors_df = pd.concat([errors_df, new_errors_df])

        except:
            new_errors_df = pd.DataFrame(
                {"StartTime": [start_date], "EndTime": [end_date], "Location": [loc]}
            )
            errors_df = pd.concat([errors_df, new_errors_df])

    # LOAD RESULTS & ERRORS TO STAGING GCS
    result_fpath = device_usg_configs["bucket"] + device_usg_configs[
        "results_stg_gcs_fpth"
    ] % (start_date, logical_dag_dt)
    error_fpath = device_usg_configs["bucket"] + device_usg_configs[
        "errors_stg_gcs_fpth"
    ] % (start_date, logical_dag_dt)
    results_df.to_csv(result_fpath, index=False)
    errors_df.to_csv(error_fpath, index=False)
    print("Wrote errors to: ", error_fpath)
    print("Wrote results to: ", result_fpath)

    # Print a summary to the console
    len_results = str(len(results_df))
    len_errors = str(len(errors_df))
    result_summary = f"# Result Rows: {len_results}; # of Error Rows: {len_errors}"
    return result_summary


def main():
    """Call the API, save data to GCS, load to BQ staging, delete & load to BQ gold"""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--cloudflare_api_token", default=cloudflare_api_token)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--dataset", default="cloudflare_derived")

    args = parser.parse_args()
    print("Running for date: ")
    print(args.date)

    # STEP 1 - Pull the data from the API, save results & errors to GCS staging area
    result_summary = get_device_usage_data(args.date, args.cloudflare_api_token)
    print("result_summary")
    print(result_summary)

    # Create a bigquery client
    client = bigquery.Client(args.project)

    result_uri = device_usg_configs["bucket"] + device_usg_configs[
        "results_stg_gcs_fpth"
    ] % (datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4), args.date)
    error_uri = device_usg_configs["bucket"] + device_usg_configs[
        "errors_stg_gcs_fpth"
    ] % (datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4), args.date)
    print("result_uri")
    print(result_uri)

    print("error_uri")
    print(error_uri)

    # STEP 2 - Copy the result data from GCS staging to BQ staging table
    load_result_csv_to_bq_stg_job = client.load_table_from_uri(
        result_uri,
        device_usg_configs["results_bq_stg_table"],
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            schema=[
                {"name": "StartTime", "type": "TIMESTAMP", "mode": "REQUIRED"},
                {"name": "UserType", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Location", "type": "STRING", "mode": "NULLABLE"},
                {"name": "DesktopUsagePct", "type": "NUMERIC", "mode": "NULLABLE"},
                {"name": "MobileUsagePct", "type": "NUMERIC", "mode": "NULLABLE"},
                {"name": "OtherUsagePct", "type": "NUMERIC", "mode": "NULLABLE"},
                {"name": "ConfLevel", "type": "STRING", "mode": "NULLABLE"},
                {"name": "AggInterval", "type": "STRING", "mode": "NULLABLE"},
                {"name": "NormalizationType", "type": "STRING", "mode": "NULLABLE"},
                {"name": "LastUpdated", "type": "TIMESTAMP", "mode": "NULLABLE"},
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        ),
    )

    load_result_csv_to_bq_stg_job.result()
    result_bq_stg_tbl = client.get_table(device_usg_configs["results_bq_stg_table"])
    print("Loaded {} rows to results staging.".format(result_bq_stg_tbl.num_rows))

    # STEP 3 - Copy the error data from GCS staging to BQ staging table
    load_error_csv_to_bq_stg_job = client.load_table_from_uri(
        error_uri,
        device_usg_configs["errors_bq_stg_table"],
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            schema=[
                {"name": "StartTime", "type": "TIMESTAMP", "mode": "REQUIRED"},
                {"name": "EndTime", "type": "TIMESTAMP", "mode": "REQUIRED"},
                {"name": "Location", "type": "STRING", "mode": "NULLABLE"},
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        ),
    )

    load_error_csv_to_bq_stg_job.result()
    error_bq_stg_tbl = client.get_table(device_usg_configs["errors_bq_stg_table"])
    print("Loaded {} rows to errors staging.".format(error_bq_stg_tbl.num_rows))

    # STEP 4 - Delete results from gold for this day, if there are any already (so if rerun, no issues will occur)
    del_exstng_gold_res_for_date = f"""DELETE FROM `moz-fx-data-shared-prod.cloudflare_derived.device_usage_v1` WHERE dte = DATE_SUB('{args.date}', INTERVAL 4 DAY)  """
    del_gold_res_job = client.query(del_exstng_gold_res_for_date)
    del_gold_res_job.result()
    print("Deleted anything already existing for this date from results gold")

    # STEP 5 - Delete errors from gold for this day, if there are any already (so if rerun, no issues will occur)
    del_exstng_gold_err_for_date = f"""DELETE FROM `moz-fx-data-shared-prod.cloudflare_derived.device_usage_errors_v1` WHERE dte = DATE_SUB('{args.date}', INTERVAL 4 DAY) """
    del_gold_err_job = client.query(del_exstng_gold_err_for_date)
    del_gold_err_job.result()
    print("Deleted anything already existing for this date from errors gold")

    # STEP 6 - Load results from stage to gold
    device_usg_stg_to_gold_query = f""" INSERT INTO `moz-fx-data-shared-prod.cloudflare_derived.device_usage_v1`
SELECT 
CAST(StartTime AS DATE) AS dte,
UserType AS user_type,
Location AS location,
DesktopUsagePct AS desktop_usage_pct,
MobileUsagePct AS mobile_usage_pct,
OtherUsagePct AS other_usage_pct,
AggInterval AS aggregation_interval,
NormalizationType AS normalization_type,
LastUpdated AS last_updated_ts 
FROM `moz-fx-data-shared-prod.cloudflare_derived.device_results_stg`
WHERE CAST(StartTime as date) = DATE_SUB('{args.date}', INTERVAL 4 DAY) """
    load_res_to_gold = client.query(device_usg_stg_to_gold_query)
    load_res_to_gold.result()

    # STEP 7 - Load errors from stage to gold
    device_usg_errors_stg_to_gold_query = f""" INSERT INTO `moz-fx-data-shared-prod.cloudflare_derived.device_usage_errors_v1`
SELECT
CAST(StartTime as date) AS dte,
Location AS location
FROM `moz-fx-data-shared-prod.cloudflare_derived.device_errors_stg`
WHERE CAST(StartTime as date) = DATE_SUB('{args.date}', INTERVAL 4 DAY) """
    load_err_to_gold = client.query(device_usg_errors_stg_to_gold_query)
    load_err_to_gold.result()

    # STEP 8 - Copy the result CSV from stage to archive, then delete from stage

    # Calculate the fpaths we will use ahead of time
    result_stg_fpath = device_usg_configs["results_stg_gcs_fpth"] % (
        datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4),
        args.date,
    )
    result_archive_fpath = device_usg_configs["results_archive_gcs_fpath"] % (
        datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4),
        args.date,
    )
    move_blob(
        device_usg_configs["bucket_no_gs"],
        result_stg_fpath,
        device_usg_configs["bucket_no_gs"],
        result_archive_fpath,
    )

    # STEP 9 - Copy the error CSV from stage to archive, then delete from stage
    error_stg_fpath = device_usg_configs["errors_stg_gcs_fpth"] % (
        datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4),
        args.date,
    )
    error_archive_fpath = device_usg_configs["errors_archive_gcs_fpath"] % (
        datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4),
        args.date,
    )
    move_blob(
        device_usg_configs["bucket_no_gs"],
        error_stg_fpath,
        device_usg_configs["bucket_no_gs"],
        error_archive_fpath,
    )


if __name__ == "__main__":
    main()
