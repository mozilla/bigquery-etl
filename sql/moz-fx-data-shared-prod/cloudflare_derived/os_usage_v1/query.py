# Load libraries
import json
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from argparse import ArgumentParser
from google.cloud import bigquery
from google.cloud import storage

# Configurations
os_usg_configs = {
    "timeout_limit": 500,
    "device_types": ["DESKTOP", "MOBILE", "OTHER", "ALL"],
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
    "results_stg_gcs_fpth": "cloudflare/os_usage/RESULTS_STAGING/%s_results_from_%s_run.csv",
    "results_archive_gcs_fpth": "cloudflare/os_usage/RESULTS_ARCHIVE/%s_results_from_%s_run.csv",
    "errors_stg_gcs_fpth": "cloudflare/os_usage/ERRORS_STAGING/%s_results_from_%s_run.csv",
    "errors_archive_gcs_fpth": "cloudflare/os_usage/ERRORS_ARCHIVE/%s_results_from_%s_run.csv",
    "gcp_project_id": "moz-fx-data-shared-prod",
    "gcp_conn_id": "google_cloud_shared_prod",
    "results_bq_stg_table": "moz-fx-data-shared-prod.cloudflare_derived.os_results_stg",
    "errors_bq_stg_table": "moz-fx-data-shared-prod.cloudflare_derived.os_errors_stg",
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


# Function to configure the API URL
def generate_os_timeseries_api_call(strt_dt, end_dt, agg_int, location, device_type):
    """Generate the API call for Operating System Usage Data."""
    if location == "ALL" and device_type == "ALL":
        os_usage_api_url = f"https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/os?dateStart={strt_dt}T00:00:00.000Z&dateEnd={end_dt}T00:00:00.000Z&format=json&aggInterval={agg_int}"
    elif location != "ALL" and device_type == "ALL":
        os_usage_api_url = f"https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/os?dateStart={strt_dt}T00:00:00.000Z&dateEnd={end_dt}T00:00:00.000Z&location={location}&format=json&aggInterval={agg_int}"
    elif location == "ALL" and device_type != "ALL":
        os_usage_api_url = f"https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/os?dateStart={strt_dt}T00:00:00.000Z&dateEnd={end_dt}T00:00:00.000Z&deviceType={device_type}&format=json&aggInterval={agg_int}"
    else:
        os_usage_api_url = f"https://api.cloudflare.com/client/v4/radar/http/timeseries_groups/os?dateStart={strt_dt}T00:00:00.000Z&dateEnd={end_dt}T00:00:00.000Z&location={location}&deviceType={device_type}&format=json&aggInterval={agg_int}"
    return os_usage_api_url


def get_os_usage_data(date_of_interest, auth_token):
    """Pull OS usage data from the Cloudflare API and save errors & results to GCS."""
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
    result_df = pd.DataFrame(
        {
            "Timestamps": [],
            "OS": [],
            "Location": [],
            "DeviceType": [],
            "Share": [],
            "ConfidenceLevel": [],
            "AggrInterval": [],
            "Normalization": [],
            "LastUpdatedTS": [],
        }
    )

    # Initialize an errors dataframe
    errors_df = pd.DataFrame(
        {"StartTime": [], "EndTime": [], "Location": [], "DeviceType": []}
    )

    # Go through all combinations, submit API requests
    for device_type in os_usg_configs["device_types"]:
        for loc in os_usg_configs["locations"]:
            print("Device Type: ", device_type)
            print("Loc: ", loc)

            # Generate the URL with given parameters
            os_usage_api_url = generate_os_timeseries_api_call(
                start_date, end_date, "1d", loc, device_type
            )
            try:
                # Call the API and save the response as JSON
                response = requests.get(
                    os_usage_api_url,
                    headers=headers,
                    timeout=os_usg_configs["timeout_limit"],
                )
                response_json = json.loads(response.text)

                # If response was successful, get the result
                if response_json["success"] is True:
                    result = response_json["result"]
                    # Parse metadata
                    conf_lvl = result["meta"]["confidenceInfo"]["level"]
                    aggr_intvl = result["meta"]["aggInterval"]
                    nrmlztn = result["meta"]["normalization"]
                    lst_upd = result["meta"]["lastUpdated"]
                    data_dict = result["serie_0"]

                    for key, val in data_dict.items():
                        if key != 'timestamps':
                            new_result_df = pd.DataFrame(
                                {
                                    "Timestamps": data_dict["timestamps"],
                                    "OS": [key] * len(val),
                                    "Location": [loc] * len(val),
                                    "DeviceType": [device_type] * len(val),
                                    "Share": val,
                                    "ConfidenceLevel": [conf_lvl] * len(val),
                                    "AggrInterval": [aggr_intvl] * len(val),
                                    "Normalization": [nrmlztn] * len(val),
                                    "LastUpdatedTS": [lst_upd] * len(val),
                                }
                            )
                            result_df = pd.concat([result_df, new_result_df])

                # If response was not successful, get the errors
                else:
                    # errors = response_json["errors"]  # Maybe add to capture, right now not using this
                    new_errors_df = pd.DataFrame(
                        {
                            "StartTime": [start_date],
                            "EndTime": [end_date],
                            "Location": [loc],
                            "DeviceType": [device_type],
                        }
                    )
                    errors_df = pd.concat([errors_df, new_errors_df])
            except:
                new_errors_df = pd.DataFrame(
                    {
                        "StartTime": [start_date],
                        "EndTime": [end_date],
                        "Location": [loc],
                        "DeviceType": [device_type],
                    }
                )
                errors_df = pd.concat([errors_df, new_errors_df])

    result_fpath = os_usg_configs["bucket"] + os_usg_configs["results_stg_gcs_fpth"] % (
        start_date,
        logical_dag_dt,
    )
    errors_fpath = os_usg_configs["bucket"] + os_usg_configs["errors_stg_gcs_fpth"] % (
        start_date,
        logical_dag_dt,
    )

    result_df.to_csv(result_fpath, index=False)
    errors_df.to_csv(errors_fpath, index=False)
    print("Wrote errors to: ", errors_fpath)
    print("Wrote results to: ", result_fpath)

    # Write a summary to the logs
    len_results = str(len(result_df))
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
    result_summary = get_os_usage_data(args.date, args.cloudflare_api_token)
    print("result_summary")
    print(result_summary)

    # Create a bigquery client
    client = bigquery.Client(args.project)

    result_uri = os_usg_configs["bucket"] + os_usg_configs["results_stg_gcs_fpth"] % (
        datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4),
        args.date,
    )
    error_uri = os_usg_configs["bucket"] + os_usg_configs["errors_stg_gcs_fpth"] % (
        datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4),
        args.date,
    )
    print("result_uri")
    print(result_uri)

    print("error_uri")
    print(error_uri)

    # STEP 2 - Copy the result data from GCS staging to BQ staging table
    load_result_csv_to_bq_stg_job = client.load_table_from_uri(
        result_uri,
        os_usg_configs["results_bq_stg_table"],
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            schema=[
                {"name": "Timestamps", "type": "TIMESTAMP", "mode": "REQUIRED"},
                {"name": "OS", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Location", "type": "STRING", "mode": "NULLABLE"},
                {"name": "DeviceType", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Share", "type": "NUMERIC", "mode": "NULLABLE"},
                {"name": "ConfidenceLevel", "type": "STRING", "mode": "NULLABLE"},
                {"name": "AggrInterval", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Normalization", "type": "STRING", "mode": "NULLABLE"},
                {"name": "LastUpdatedTS", "type": "TIMESTAMP", "mode": "NULLABLE"},
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        ),
    )

    load_result_csv_to_bq_stg_job.result()
    result_bq_stg_tbl = client.get_table(os_usg_configs["results_bq_stg_table"])
    print("Loaded {} rows to results staging.".format(result_bq_stg_tbl.num_rows))

    # STEP 3 - Copy the error data from GCS staging to BQ staging table
    load_error_csv_to_bq_stg_job = client.load_table_from_uri(
        error_uri,
        os_usg_configs["errors_bq_stg_table"],
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            schema=[
                {"name": "StartDate", "type": "DATE", "mode": "REQUIRED"},
                {"name": "EndDate", "type": "DATE", "mode": "REQUIRED"},
                {"name": "Location", "type": "STRING", "mode": "NULLABLE"},
                {"name": "DeviceType", "type": "STRING", "mode": "NULLABLE"},
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        ),
    )

    load_error_csv_to_bq_stg_job.result()
    error_bq_stg_tbl = client.get_table(os_usg_configs["errors_bq_stg_table"])
    print("Loaded {} rows to errors staging.".format(error_bq_stg_tbl.num_rows))

    # STEP 4 - Delete results from gold for this day, if there are any already (so if rerun, no issues will occur)
    del_exstng_gold_res_for_date = f"""DELETE FROM `moz-fx-data-shared-prod.cloudflare_derived.os_usage_v1` WHERE dte = DATE_SUB('{args.date}', INTERVAL 4 DAY)  """
    del_gold_res_job = client.query(del_exstng_gold_res_for_date)
    del_gold_res_job.result()
    print("Deleted anything already existing for this date from results gold")

    # STEP 5 - Delete errors from gold for this day, if there are any already (so if rerun, no issues will occur)
    del_exstng_gold_err_for_date = f"""DELETE FROM `moz-fx-data-shared-prod.cloudflare_derived.os_usage_errors_v1` WHERE dte = DATE_SUB('{args.date}', INTERVAL 4 DAY) """
    del_gold_err_job = client.query(del_exstng_gold_err_for_date)
    del_gold_err_job.result()
    print("Deleted anything already existing for this date from errors gold")

    # STEP 6 - Load results from stage to gold # NEED TO UPDATE THIS STILL
    os_usg_stg_to_gold_query = f""" INSERT INTO `moz-fx-data-shared-prod.cloudflare_derived.os_usage_v1`
SELECT 
CAST(Timestamps AS date) AS dte,
OS AS os,
Location AS location,
DeviceType AS device_type,
Share AS os_share,
Normalization AS normalization_type,
LastUpdatedTS AS last_updated_ts
FROM `moz-fx-data-shared-prod.cloudflare_derived.os_results_stg`
WHERE CAST(Timestamps as date) = DATE_SUB('{args.date}', INTERVAL 4 DAY) """
    load_res_to_gold = client.query(os_usg_stg_to_gold_query)
    load_res_to_gold.result()

    # STEP 7 - Load errors from stage to gold
    os_usg_errors_stg_to_gold_query = f""" INSERT INTO `moz-fx-data-shared-prod.cloudflare_derived.os_usage_errors_v1`
SELECT
StartDate AS dte,
Location AS location,
DeviceType AS device_type
FROM `moz-fx-data-shared-prod.cloudflare_derived.os_errors_stg`
WHERE StartDate = DATE_SUB('{args.date}', INTERVAL 4 DAY) """
    load_err_to_gold = client.query(os_usg_errors_stg_to_gold_query)
    load_err_to_gold.result()

    # STEP 8 - Copy the result CSV from stage to archive, then delete from stage
    # Calculate the fpaths we will use ahead of time
    result_stg_fpath = os_usg_configs["results_stg_gcs_fpth"] % (
        datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4),
        args.date,
    )
    result_archive_fpath = os_usg_configs["results_archive_gcs_fpth"] % (
        datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4),
        args.date,
    )
    move_blob(
        os_usg_configs["bucket_no_gs"],
        result_stg_fpath,
        os_usg_configs["bucket_no_gs"],
        result_archive_fpath,
    )

    # STEP 9 - Copy the error CSV from stage to archive, then delete from stage
    error_stg_fpath = os_usg_configs["errors_stg_gcs_fpth"] % (
        datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4),
        args.date,
    )
    error_archive_fpath = os_usg_configs["errors_archive_gcs_fpth"] % (
        datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4),
        args.date,
    )
    move_blob(
        os_usg_configs["bucket_no_gs"],
        error_stg_fpath,
        os_usg_configs["bucket_no_gs"],
        error_archive_fpath,
    )


if __name__ == "__main__":
    main()
