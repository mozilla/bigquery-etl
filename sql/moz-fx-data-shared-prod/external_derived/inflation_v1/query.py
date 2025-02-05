# Load libraries
from datetime import datetime, timedelta
import pandas as pd
import requests
from argparse import ArgumentParser
from google.cloud import bigquery
import time
import pandas_gbq

# Set variables
countries = ["AU", "CA", "CH", "GB", "FR", "ES", "US", "DE", "IT", "JP", "PL"]
START_LOOKBACK_DAYS = 1825
END_LOOKBACK_DAYS = 15
WAIT_TIME_SECONDS = 10
TARGET_PROJECT = "moz-fx-data-shared-prod"
TARGET_TABLE = "external_derived.inflation_v1"


# Define function to pull CPI data
def pull_monthly_cpi_data_from_imf(country_code, start_month, end_month):
    """
    Inputs:
        Country Code - 2 letter country code, for example, US
        Start Month - YYYY-MM - for example, 2023-10
        End Month - YYYY-MM - for example, 2023-12

    Output:
      JSON with data for this country for the months between start month and end month
    """
    api_url = f"http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/M.{country_code}.PCPI_IX.?startPeriod={start_month}&endPeriod={end_month}"

    response = requests.get(api_url, timeout=10)
    inflation_data = response.json()

    series = (
        inflation_data.get("CompactData", {}).get("DataSet", {}).get("Series", None)
    )

    observations = series.get("Obs", [])
    observations_df = pd.DataFrame(observations)
    observations_df["country"] = country_code

    # Rename to friendlier names
    observations_df.rename(
        columns={"@TIME_PERIOD": "report_period", "@OBS_VALUE": "consumer_price_index"},
        inplace=True,
    )

    # Reorder cols to match schema order
    observations_df = observations_df[
        ["report_period", "consumer_price_index", "country"]
    ]
    return observations_df


def main():
    """Call the API, save data to GCS, load to BQ staging, delete & load to BQ gold"""
    today = datetime.today()
    curr_date = today.strftime("%Y-%m-%d")
    print("curr_date")
    print(curr_date)

    # Calculate start month = month 13 months ago
    start_month_stg = today - timedelta(days=START_LOOKBACK_DAYS)
    start_month = start_month_stg.replace(day=1).strftime("%Y-%m")
    print("start_month: ", start_month)

    # Calculate end month = month 1 month ago
    end_month_stg = today.replace(day=1) - timedelta(days=END_LOOKBACK_DAYS)
    end_month = end_month_stg.strftime("%Y-%m")
    print("end_month: ", end_month)

    # Initialize a results dataframe
    results_df = pd.DataFrame(
        {"report_period": [], "consumer_price_index": [], "country": []}
    )

    # For each country
    for country in countries:
        # Pull the CPI data
        curr_country_infl_df = pull_monthly_cpi_data_from_imf(
            country, start_month, end_month
        )

        # Append it to the results dataframe
        results_df = pd.concat([results_df, curr_country_infl_df])

        # Sleep between each call since the API is rate limited
        time.sleep(WAIT_TIME_SECONDS)

    # Add a column with the current date
    results_df["last_updated"] = curr_date

    # Write the final results_df to BQ external_derived.inflation_v1 table
    # This will always over-write with the latest data
    pandas_gbq.to_gbq(
        results_df,
        TARGET_TABLE,
        project_id=TARGET_PROJECT,
        if_exists="replace",
    )


if __name__ == "__main__":
    main()
