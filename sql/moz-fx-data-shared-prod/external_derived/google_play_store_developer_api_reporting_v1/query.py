import os
import json
import google.auth
from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2 import service_account
import requests
from argparse import ArgumentParser
from google.cloud import bigquery
import pandas as pd

# Set variables
API_URL = "https://playdeveloperreporting.googleapis.com/v1beta1/apps/org.mozilla.firefox/%s"

metrics_of_interest = {'slowStartRateMetricSet': None} 

request_payload = request_payload = {
    "timelineSpec": {
        "aggregationPeriod": "DAILY",
        "startTime": {"year": 2025, "month": 3, "day": 16},
        "endTime": {"year": 2025, "month": 3, "day": 17}
    },
    "metrics": ["slowStartRate"],
    "dimensions": ["startType", "countryCode", "versionCode"],  # REQUIRED dimension startType, others are optional
    "pageSize": 100  # Adjust as needed
}


TARGET_PROJECT = "moz-fx-data-shared-prod"
TARGET_TABLE = "moz-fx-data-shared-prod.external_derived.google_play_store_developer_api_reporting_v1"
GCS_BUCKET = "gs://moz-fx-data-prod-external-data/"
RESULTS_FPATH = "GOOGLE_PLAY_STORE/google_play_store_developer_api_reporting_%s.csv"
TIMEOUT_IN_SECONDS = 10
SERVICE_ACCOUNT_FILE = "/Users/kwindau/Documents/2025/202502/boxwood-axon-825-a8f9a0239d65.json"  #######TEMP - will replace later on when moved to airflow###########
SCOPES = ["https://www.googleapis.com/auth/playdeveloperreporting"]


def get_metric_set_metadata(access_token, url, metric_set, timeout_seconds):
    """Call the API URL using the given credentials and the given timeout limit
    Inputs:
    * Access Token
    * URL (string)
    * Metric Set
    * Timeout Seconds (int)
    Outputs:
    * API call response"""
    full_url = url % (metric_set)
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    response = requests.get(full_url, headers=headers, timeout=timeout_seconds)
    return response


def parse_metric_set_metadata(metric_set_metadata_json):
    """Takes a JSON, returns 2 pandas dataframes = 1 for hourly info, 1 for daily info"""

    metric_set_daily_metadata_df = pd.DataFrame(
        {
            "latest_end_time_year": [],
            "latest_end_time_month": [],
            "latest_end_time_day": [],
            "latest_end_time_timezone": [],
        }
    )
    for freshness in metric_set_metadata_json["freshnessInfo"]["freshnesses"]:
        agg_period = freshness["aggregationPeriod"]
        if agg_period == "DAILY":
            latest_end_time_year = freshness["latestEndTime"]["year"]
            latest_end_time_month = freshness["latestEndTime"]["month"]
            latest_end_time_day = freshness["latestEndTime"]["day"]
            latest_end_time_tz = freshness["latestEndTime"]["timeZone"]["id"]
            new_daily_df = pd.DataFrame(
                {
                    "latest_end_time_year": [latest_end_time_year],
                    "latest_end_time_month": [latest_end_time_month],
                    "latest_end_time_day": [latest_end_time_day],
                    "latest_end_time_timezone": [latest_end_time_tz],
                }
            )
            metric_set_daily_metadata_df = pd.concat(
                [metric_set_daily_metadata_df, new_daily_df]
            )

    #return metric_set_hourly_metadata_df, metric_set_daily_metadata_df
    return metric_set_daily_metadata_df



def pull_metric_data(token, metric_set_to_query, timeout_seconds):
    url = f"https://playdeveloperreporting.googleapis.com/v1beta1/apps/org.mozilla.firefox/{metric_set_to_query}:query"
    
    headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
    }

    response = requests.post(url, headers=headers, json=request_payload, timeout=timeout_seconds)
    return response

def main():
    """Call the API, save data to GCS, write data to BQ"""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    #Get the DAG logical run date
    logical_dag_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    logical_dag_date_string = logical_dag_date.strftime("%Y-%m-%d")
    print("logical_dag_date_string: ", logical_dag_date_string)

    #Get today's date
    today = datetime.today()
    curr_date = today.strftime("%Y-%m-%d")
    print("curr_date")
    print(curr_date)

    # Get credentials 
    # #######TEMP BELOW - will replace later on when moved to airflow ###########
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    credentials.refresh(Request())
    access_token = credentials.token
    # ####### TEMP ABOVE - will replace later on when moved to airflow #### 

    # Loop through the metrics_of_interest dictionary
    for metric_set, metric in metrics_of_interest.items():
      #Print the current key and value
      print('Current Metric Set: ', metric_set)
      print('Current Metric: ', metric)
      #Get the freshness metadata for this metric set
      metricset_metadata = get_metric_set_metadata(
          access_token, API_URL, metric_set, TIMEOUT_IN_SECONDS
      )
      metricset_metadata_json = metricset_metadata.json()
      print("metricset_metadata_json")
      print(metricset_metadata_json)

      metadata_daily_df = parse_metric_set_metadata(
         metricset_metadata_json
      )

      print("metadata_daily_df")
      print(metadata_daily_df)

      new_results = pull_metric_data(token=access_token,
                             metric_set_to_query=metric_set,
                             timeout_seconds=TIMEOUT_IN_SECONDS)
      print('new_results')
      print(new_results)
      print(new_results.text)

      # Now, let's query the metric sets and put into a final_df
      #final_df = #??

    # #Let's load the data to CSV in GCS
    # final_results_fpath = GCS_BUCKET + RESULTS_FPATH % (curr_date)
    # print("final_results_fpath: ", final_results_fpath)
    # final_df.to_csv(final_results_fpath, index=False)

    #Let's copy it from the CSV to BQ
    # client = bigquery.Client(TARGET_PROJECT)
    # load_csv_to_gcp_job = client.load_table_from_uri(
    #     final_results_fpath,
    #     TARGET_TABLE,
    #     job_config=bigquery.LoadJobConfig(
    #         create_disposition="CREATE_NEVER",
    #         write_disposition="WRITE_TRUNCATE",
    #         schema=[
    #             {"name": "submission_date", "type": "DATE", "mode": "NULLABLE"},
    #         ],
    #         skip_leading_rows=1,
    #         source_format=bigquery.SourceFormat.CSV,
    #     ),
    # )

    # load_csv_to_gcp_job.result()    


if __name__ == "__main__":
    main()
