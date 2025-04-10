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
brwsr_usg_configs = {
    "timeout_limit": 220,
    "device_types": ["DESKTOP", "MOBILE", "OTHER", "ALL"],
    "max_limit": 20,
    "operating_systems": [
        "ALL",
        "WINDOWS",
        "MACOSX",
        "IOS",
        "ANDROID",
        "CHROMEOS",
        "LINUX",
        "SMART_TV",
    ],
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
        "JP",
        "KE",
        "IT",
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
    "user_types": ["ALL", "LIKELY_AUTOMATED", "LIKELY_HUMAN"],
    "bucket": "gs://moz-fx-data-prod-external-data/",
    "bucket_no_gs": "moz-fx-data-prod-external-data",
    "results_stg_gcs_fpth": "cloudflare/browser_usage/RESULTS_STAGING/%s_results_from_%s_run.csv",
    "results_archive_gcs_fpath": "cloudflare/browser_usage/RESULTS_ARCHIVE/%s_results_from_%s_run.csv",
    "errors_stg_gcs_fpth": "cloudflare/browser_usage/ERRORS_STAGING/%s_errors_from_%s_run.csv",
    "errors_archive_gcs_fpath": "cloudflare/browser_usage/ERRORS_ARCHIVE/%s_errors_from_%s_run.csv",
    "gcp_project_id": "moz-fx-data-shared-prod",
    "gcp_conn_id": "google_cloud_shared_prod",
    "results_bq_stg_table": "moz-fx-data-shared-prod.cloudflare_derived.browser_results_stg",
    "errors_bq_stg_table": "moz-fx-data-shared-prod.cloudflare_derived.browser_errors_stg",
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


# Define function to generate API call based on configs passed
def generate_browser_api_call(
    strt_dt, end_dt, device_type, location, op_system, user_typ, limit
):
    """Create the API url based on the input parameters."""
    user_type_string = "" if user_typ == "ALL" else f"&botClass={user_typ}"
    location_string = "" if location == "ALL" else f"&location={location}"
    op_system_string = "" if op_system == "ALL" else f"&os={op_system}"
    device_type_string = "" if device_type == "ALL" else f"&deviceType={device_type}"
    browser_api_url = f"https://api.cloudflare.com/client/v4/radar/http/top/browser?dateStart={strt_dt}T00:00:00.000Z&dateEnd={end_dt}T00:00:00.000Z{device_type_string}{location_string}{op_system_string}{user_type_string}&limit={limit}&format=json"
    return browser_api_url


# Define function to pull browser data from the cloudflare API
def get_browser_data(date_of_interest, auth_token):
    """Pull browser data for each combination of the configs from the Cloudflare API, always runs with a lag of 4 days."""
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
    limit = brwsr_usg_configs["max_limit"]

    # Initialize the empty results and errors dataframes
    browser_results_df = pd.DataFrame(
        {
            "StartTime": [],
            "EndTime": [],
            "DeviceType": [],
            "Location": [],
            "UserType": [],
            "Browser": [],
            "OperatingSystem": [],
            "PercentShare": [],
            "ConfLevel": [],
            "Normalization": [],
            "LastUpdated": [],
        }
    )

    browser_errors_df = pd.DataFrame(
        {
            "StartTime": [],
            "EndTime": [],
            "Location": [],
            "UserType": [],
            "DeviceType": [],
            "OperatingSystem": [],
        }
    )

    # Loop through the combinations
    for device_type in brwsr_usg_configs["device_types"]:
        for loc in brwsr_usg_configs["locations"]:
            for os in brwsr_usg_configs["operating_systems"]:
                for user_type in brwsr_usg_configs["user_types"]:
                    curr_combo = f"DeviceType: {device_type}, Loc: {loc}, OS: {os}, UserType: {user_type}, Limit: {limit}"
                    print(curr_combo)

                    # Generate the URL & call the API
                    brwsr_usg_api_url = generate_browser_api_call(
                        start_date, end_date, device_type, loc, os, user_type, limit
                    )
                    try:
                        response = requests.get(
                            brwsr_usg_api_url,
                            headers=headers,
                            timeout=brwsr_usg_configs["timeout_limit"],
                        )
                        response_json = json.loads(response.text)

                        # if the response was successful, get the result and append it to the results dataframe
                        if response_json["success"] is True:
                            # Save the results to GCS
                            result = response_json["result"]
                            confidence_level = result["meta"]["confidenceInfo"]["level"]
                            normalization = result["meta"]["normalization"]
                            last_updated = result["meta"]["lastUpdated"]
                            startTime = result["meta"]["dateRange"][0]["startTime"]
                            endTime = result["meta"]["dateRange"][0]["endTime"]
                            data = result["top_0"]
                            browser_lst = []
                            browser_share_lst = []

                            for browser in data:
                                browser_lst.append(browser["name"])
                                browser_share_lst.append(browser["value"])

                            new_browser_results_df = pd.DataFrame(
                                {
                                    "StartTime": [startTime] * len(browser_lst),
                                    "EndTime": [endTime] * len(browser_lst),
                                    "DeviceType": [device_type] * len(browser_lst),
                                    "Location": [loc] * len(browser_lst),
                                    "UserType": [user_type] * len(browser_lst),
                                    "Browser": browser_lst,
                                    "OperatingSystem": [os] * len(browser_lst),
                                    "PercentShare": browser_share_lst,
                                    "ConfLevel": [confidence_level] * len(browser_lst),
                                    "Normalization": [normalization] * len(browser_lst),
                                    "LastUpdated": [last_updated] * len(browser_lst),
                                }
                            )
                            browser_results_df = pd.concat(
                                [browser_results_df, new_browser_results_df]
                            )

                        # If there were errors, save them to the errors dataframe
                        else:
                            new_browser_error_df = pd.DataFrame(
                                {
                                    "StartTime": [start_date],
                                    "EndTime": [end_date],
                                    "Location": [loc],
                                    "UserType": [user_type],
                                    "DeviceType": [device_type],
                                    "OperatingSystem": [os],
                                }
                            )
                            browser_errors_df = pd.concat(
                                [browser_errors_df, new_browser_error_df]
                            )
                    except:
                        new_browser_error_df = pd.DataFrame(
                            {
                                "StartTime": [start_date],
                                "EndTime": [end_date],
                                "Location": [loc],
                                "UserType": [user_type],
                                "DeviceType": [device_type],
                                "OperatingSystem": [os],
                            }
                        )
                        browser_errors_df = pd.concat(
                            [browser_errors_df, new_browser_error_df]
                        )

    # LOAD RESULTS & ERRORS TO STAGING GCS
    result_fpath = brwsr_usg_configs["bucket"] + brwsr_usg_configs[
        "results_stg_gcs_fpth"
    ] % (start_date, logical_dag_dt)
    error_fpath = brwsr_usg_configs["bucket"] + brwsr_usg_configs[
        "errors_stg_gcs_fpth"
    ] % (start_date, logical_dag_dt)
    browser_results_df.to_csv(result_fpath, index=False)
    browser_errors_df.to_csv(error_fpath, index=False)
    print("Wrote errors to: ", error_fpath)
    print("Wrote results to: ", result_fpath)

    # Return a summary to the console
    len_results = str(len(browser_results_df))
    len_errors = str(len(browser_errors_df))
    result_summary = [len_results, len_errors]
    return result_summary


def main():
    """Call the API, save data to GCS, load to BQ staging, delete & load to BQ gold"""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--cloudflare_api_token", default=cloudflare_api_token)
    parser.add_argument("--project", default=brwsr_usg_configs["gcp_project_id"])
    parser.add_argument("--dataset", default="cloudflare_derived")

    args = parser.parse_args()
    print("Running for date: ")
    print(args.date)

    # STEP 1 - Pull the data from the API, save results & errors to GCS staging area
    results = get_browser_data(args.date, args.cloudflare_api_token)
    nbr_successful = results[0]
    nbr_errors = results[1]
    result_summary = f"# Result Rows: {nbr_successful}; # of Error Rows: {nbr_errors}"
    print("result_summary")
    print(result_summary)

    # Create a bigquery client
    client = bigquery.Client(args.project)

    result_uri = brwsr_usg_configs["bucket"] + brwsr_usg_configs[
        "results_stg_gcs_fpth"
    ] % (datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4), args.date)
    error_uri = brwsr_usg_configs["bucket"] + brwsr_usg_configs[
        "errors_stg_gcs_fpth"
    ] % (datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4), args.date)
    print("result_uri")
    print(result_uri)

    print("error_uri")
    print(error_uri)

    # STEP 2 - Copy the result data from GCS staging to BQ staging table
    load_result_csv_to_bq_stg_job = client.load_table_from_uri(
        result_uri,
        brwsr_usg_configs["results_bq_stg_table"],
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            schema=[
                {"name": "StartTime", "type": "TIMESTAMP", "mode": "REQUIRED"},
                {"name": "EndTime", "type": "TIMESTAMP", "mode": "REQUIRED"},
                {"name": "DeviceType", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Location", "type": "STRING", "mode": "NULLABLE"},
                {"name": "UserType", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Browser", "type": "STRING", "mode": "NULLABLE"},
                {"name": "OperatingSystem", "type": "STRING", "mode": "NULLABLE"},
                {"name": "PercentShare", "type": "NUMERIC", "mode": "NULLABLE"},
                {"name": "ConfLevel", "type": "STRING", "mode": "NULLABLE"},
                {"name": "Normalization", "type": "STRING", "mode": "NULLABLE"},
                {"name": "LastUpdated", "type": "TIMESTAMP", "mode": "NULLABLE"},
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        ),
    )
    load_result_csv_to_bq_stg_job.result()
    result_bq_stg_tbl = client.get_table(brwsr_usg_configs["results_bq_stg_table"])
    print("Loaded {} rows to results staging.".format(result_bq_stg_tbl.num_rows))

    # STEP 3 - Copy the error data from GCS staging to BQ staging table
    load_error_csv_to_bq_stg_job = client.load_table_from_uri(
        error_uri,
        brwsr_usg_configs["errors_bq_stg_table"],
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_TRUNCATE",
            schema=[
                {"name": "StartTime", "type": "DATE", "mode": "REQUIRED"},
                {"name": "EndTime", "type": "DATE", "mode": "REQUIRED"},
                {"name": "Location", "type": "STRING", "mode": "NULLABLE"},
                {"name": "UserType", "type": "STRING", "mode": "NULLABLE"},
                {"name": "DeviceType", "type": "STRING", "mode": "NULLABLE"},
                {"name": "OperatingSystem", "type": "STRING", "mode": "NULLABLE"},
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        ),
    )

    load_error_csv_to_bq_stg_job.result()
    error_bq_stg_tbl = client.get_table(brwsr_usg_configs["errors_bq_stg_table"])
    print("Loaded {} rows to errors staging.".format(error_bq_stg_tbl.num_rows))

    # STEP 4 - Delete results from gold for this day, if there are any already (so if rerun, no issues will occur)
    del_exstng_gold_res_for_date = f"""DELETE FROM `moz-fx-data-shared-prod.cloudflare_derived.browser_usage_v1` WHERE dte = DATE_SUB('{args.date}', INTERVAL 4 DAY)  """
    del_gold_res_job = client.query(del_exstng_gold_res_for_date)
    del_gold_res_job.result()
    print("Deleted anything already existing for this date from results gold")

    # STEP 5 - Delete errors from gold for this day, if there are any already (so if rerun, no issues will occur)
    del_exstng_gold_err_for_date = f"""DELETE FROM `moz-fx-data-shared-prod.cloudflare_derived.browser_usage_errors_v1` WHERE dte = DATE_SUB('{args.date}', INTERVAL 4 DAY) """
    del_gold_err_job = client.query(del_exstng_gold_err_for_date)
    del_gold_err_job.result()
    print("Deleted anything already existing for this date from errors gold")

    # STEP 6 - Load results from stage to gold
    browser_usg_stg_to_gold_query = f""" INSERT INTO `moz-fx-data-shared-prod.cloudflare_derived.browser_usage_v1`
SELECT
CAST(StartTime as date) AS dte,
DeviceType AS device_type,
Location AS location,
UserType AS user_type,
Browser AS browser,
OperatingSystem AS operating_system,
PercentShare AS percent_share,
Normalization AS normalization,
LastUpdated AS last_updated_ts
FROM `moz-fx-data-shared-prod.cloudflare_derived.browser_results_stg`
WHERE CAST(StartTime as date) = DATE_SUB('{args.date}', INTERVAL 4 DAY) """
    load_res_to_gold = client.query(browser_usg_stg_to_gold_query)
    load_res_to_gold.result()

    # STEP 7 - Load errors from stage to gold
    browser_usg_errors_stg_to_gold_query = f""" INSERT INTO `moz-fx-data-shared-prod.cloudflare_derived.browser_usage_errors_v1`
SELECT
CAST(StartTime as date) AS dte,
Location AS location,
UserType AS user_type,
DeviceType AS device_type,
OperatingSystem AS operating_system
FROM `moz-fx-data-shared-prod.cloudflare_derived.browser_errors_stg`
WHERE CAST(StartTime as date) = DATE_SUB('{args.date}', INTERVAL 4 DAY) """
    load_err_to_gold = client.query(browser_usg_errors_stg_to_gold_query)
    load_err_to_gold.result()

    # STEP 8 - Copy the result CSV from stage to archive, then delete from stage

    # Calculate the fpaths we will use ahead of time
    result_stg_fpath = brwsr_usg_configs["results_stg_gcs_fpth"] % (
        datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4),
        args.date,
    )
    result_archive_fpath = brwsr_usg_configs["results_archive_gcs_fpath"] % (
        datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4),
        args.date,
    )
    move_blob(
        brwsr_usg_configs["bucket_no_gs"],
        result_stg_fpath,
        brwsr_usg_configs["bucket_no_gs"],
        result_archive_fpath,
    )

    # STEP 9 - Copy the error CSV from stage to archive, then delete from stage
    error_stg_fpath = brwsr_usg_configs["errors_stg_gcs_fpth"] % (
        datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4),
        args.date,
    )
    error_archive_fpath = brwsr_usg_configs["errors_archive_gcs_fpath"] % (
        datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=4),
        args.date,
    )
    move_blob(
        brwsr_usg_configs["bucket_no_gs"],
        error_stg_fpath,
        brwsr_usg_configs["bucket_no_gs"],
        error_archive_fpath,
    )

    #If # errors > 200 (more than 10%), fail with error
    if int(nbr_errors) > 200:
        raise Exception("200 or more errors, check for issues")


if __name__ == "__main__":
    main()
