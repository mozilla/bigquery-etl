# Load libraries
from datetime import datetime, timedelta
import time
import pandas as pd
import requests
from google.cloud import bigquery
import sys

# Define inputs
country_codes = [
    "AU",
    "CH",
    "GB",
    "US",
    "BR",
    "CA",
    "CN",
    "CO",
    "IN",
    "IS",
    "JP",
    "MX",
    "PL",
    "TR",
]
LOOKBACK_YEARS = 5
TARGET_PROJECT = "moz-fx-data-shared-prod"
TARGET_TABLE = "moz-fx-data-shared-prod.external_derived.imf_exchange_rates_v1"
GCS_BUCKET = "gs://moz-fx-data-prod-external-data/"
GCS_BUCKET_NO_GS = "moz-fx-data-prod-external-data"
RESULTS_FPATH = "IMF_EXCHANGE_RATES/imf_exchange_rate_data_%s.csv"
WAIT_TIME_SECONDS = 5


# Define a function to load exchange rate data
def fetch_exchange_rate_data(
    base_url, frequency, country_code, start_period=None, end_period=None
):
    """
    Fetch exchange rate data (relative to USD) from the IMF API and return as a pandas DataFrame.

    Parameters:
        base_url (str): Base URL of the IMF API.
        frequency (str): Data frequency (e.g., 'M' for monthly, 'Q' for quarterly, 'A' for annual).
        country_code (str): ISO country code (e.g., 'GB' for the United Kingdom).
        start_period (str, optional): Start date for the series in 'YYYY' or 'YYYY-MM' format.
        end_period (str, optional): End date for the series in 'YYYY' or 'YYYY-MM' format.

    Returns:
        pd.DataFrame: A DataFrame containing the date, exchange rate, frequency, and country.
    """
    # Construct the dataset key dynamically
    dataset_key = f"{frequency}.{country_code}.ENDA_XDC_USD_RATE"

    # Fetch data from the API
    data_url = f"{base_url}{dataset_key}"
    params = {
        "observations": 1,  # include observations in the payload
        "format": "json",  # optional; JSON is the default
    }
    response = requests.get(data_url, params=params, timeout=60)
    response.raise_for_status()
    payload = response.json()

    # Extract the data
    docs = payload.get("series", {}).get("docs", [])
    if not docs:
        return pd.DataFrame(columns=["period", "value"])

    doc = docs[0]
    periods = doc.get("period", [])
    values = doc.get("value", [])

    df = pd.DataFrame({"period": periods, "value": values})

    # Coerce to monthly periods, drop missing, sort ascending
    df["period"] = pd.PeriodIndex(df["period"], freq="M")
    df = df.dropna(subset=["value"]).sort_values("period").reset_index(drop=True)

    # Add a column for first date of month
    df["report_month"] = pd.PeriodIndex(df["period"], freq="M")

    filtered_df = df[
        df["report_month"].between(
            pd.Period(start_period, "M"), pd.Period(end_period, "M")
        )
    ]

    # Add labels
    filtered_df = filtered_df[["period", "value"]]
    filtered_df = filtered_df.rename(
        columns={"period": "report_period", "value": "exchange_rate"}
    )
    filtered_df["frequency"] = "M"
    filtered_df["country_code"] = country_code

    return filtered_df


def initialize_results_df():
    """Returns a dataframe with 0 rows with the desired format"""
    results_df = pd.DataFrame(
        columns=["report_period", "exchange_rate", "frequency", "country_code"]
    )
    return results_df


def calculate_start_period(curr_date, nbr_years_to_go_back):
    """Calculates start period relative to # years back worth of data to pull
    Input:
    * curr_date - string date in YYYY-MM-DD format
    * nbr_years_to_go_back - Integer representing number of years back from the current date
    Output: YYYY-MM string for the month that many years back from the current date
    """

    # Calculate start period date based on subtracting nbr_years_to_go_back * 365 from the current date
    start_period_date = curr_date - timedelta(days=nbr_years_to_go_back * 365)
    start_period = start_period_date.replace(day=1).strftime("%Y-%m")
    return start_period


def calculate_end_period(curr_date):
    """Calculates end period
    Input: Current date as a string in YYYY-MM-DD format
    Output: YYYY-MM string of the month as of 15 days ago
    """
    # Calculate end month = month that was 30 days ago
    end_period_date = curr_date.replace(day=1) - timedelta(days=30)
    end_period = end_period_date.strftime("%Y-%m")
    return end_period


def main():
    """Pull exchange rate relative to US dollar for each of the country codes, load to GCS then BQ"""
    base_url = f"https://api.db.nomics.world/v22/series/IMF/IFS/"
    frequency = "M"  # Monthly data

    # Get today's date
    today = datetime.today()
    curr_date = today.strftime("%Y-%m-%d")
    print("curr_date: ", str(curr_date))

    # Get start & end period
    start_period = calculate_start_period(today, LOOKBACK_YEARS)
    end_period = calculate_end_period(today)
    print("Start Period: ", str(start_period))
    print("End Period: ", str(end_period))

    # Initialize the data frame
    results_df = initialize_results_df()

    # For each country, call the API and get the data and append it to results_df
    for country_code in country_codes:
        print("country_code: ", str(country_code))
        df = fetch_exchange_rate_data(
            base_url,
            frequency,
            country_code,
            start_period=start_period,
            end_period=end_period,
        )

        # Append the results to the dataframe
        results_df = pd.concat([results_df, df])

        # Wait a little before the next request
        time.sleep(WAIT_TIME_SECONDS)

    # Append it as a column on the final dataframe
    results_df["last_updated"] = curr_date

    # Write the results to CSV in GCS
    final_results_fpath = GCS_BUCKET + RESULTS_FPATH % curr_date
    results_df.to_csv(final_results_fpath, index=False)

    # Load to BQ
    client = bigquery.Client(TARGET_PROJECT)
    load_csv_to_gdp_v1_job = client.load_table_from_uri(
        final_results_fpath,
        TARGET_TABLE,
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_NEVER",
            write_disposition="WRITE_TRUNCATE",
            schema=[
                {"name": "report_period", "type": "STRING", "mode": "NULLABLE"},
                {"name": "exchange_rate", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "frequency", "type": "STRING", "mode": "NULLABLE"},
                {"name": "country_code", "type": "STRING", "mode": "NULLABLE"},
                {"name": "last_updated", "type": "DATE", "mode": "NULLABLE"},
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        ),
    )

    load_csv_to_gdp_v1_job.result()


if __name__ == "__main__":
    main()
