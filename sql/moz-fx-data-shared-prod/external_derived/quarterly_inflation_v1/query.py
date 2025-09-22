# Load libraries
from datetime import datetime, timedelta
import time
import pandas as pd
import requests
from google.cloud import bigquery

# Set variables
countries = ["AU", "CN", "FI"]
START_LOOKBACK_DAYS = 1825
END_LOOKBACK_DAYS = 15
WAIT_TIME_SECONDS = 30
TARGET_PROJECT = "moz-fx-data-shared-prod"
TARGET_TABLE = "moz-fx-data-shared-prod.external_derived.quarterly_inflation_v1"
GCS_BUCKET = "gs://moz-fx-data-prod-external-data/"
GCS_BUCKET_NO_GS = "moz-fx-data-prod-external-data"
RESULTS_FPATH = "IMF_QUARTERLY_CPI/imf_quarterly_cpi_data_%s.csv"


# Define a function to rebase data to the mean of 2010
def rebase_to_2010(df, value_col="consumer_price_index", period_col="report_period"):
    df = df.copy()
    dt = pd.to_datetime(df[period_col], errors="coerce")

    mask_2010 = dt.notna() & (dt.dt.year == 2010)
    y2010 = pd.to_numeric(df.loc[mask_2010, value_col], errors="coerce")

    if y2010.dropna().empty:
        raise ValueError("No 2010 observations to compute a 2010=100 base.")

    factor = 100.0 / y2010.mean()  # annual average 2010 -> 100
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce") * factor
    df["base_year"] = "2010=100"
    return df


# Define function to pull CPI data
def pull_quarterly_cpi_data_from_imf(country_code, start_month):
    """
    Inputs:
        Country Code - 2 letter country code, for example, US
        Start Month - YYYY-MM - for example, 2023-10

    Output:
      Dataframe for this country for the quarters from the year of the start month, to the present
    """
    api_url = f"https://api.db.nomics.world/v22/series/IMF/CPI/Q.{country_code}.PCPI_IX"
    params = {
        "observations": 1,  # include observations in the payload
        "format": "json",  # optional; JSON is the default
    }

    response = requests.get(api_url, params=params, timeout=20)
    inflation_data = response.json()
    doc = (inflation_data.get("series", {}) or {}).get("docs", [None])[0]
    if doc is None:
        raise ValueError("Series doc not found in payload")

    # Build a tidy DataFrame
    observations_df = pd.DataFrame(
        {
            "report_period": doc["period"],
            "consumer_price_index": pd.to_numeric(doc["value"], errors="coerce"),
            "country": [country_code] * len(doc["period"]),
        }
    )

    observations_rebased_to_2010 = rebase_to_2010(
        observations_df, value_col="consumer_price_index", period_col="report_period"
    )

    observations_rebased_to_2010["base_year"] = "2010=100"

    # Filter to only quarters in year of start month or greater
    observations_rebased_to_2010["report_year"] = (
        observations_rebased_to_2010["report_period"].astype(str).str[:4]
    )

    # Get the string version of the start month
    start_year = int(start_month[:4])

    # Filter to year of start month forward
    filtered = observations_rebased_to_2010.loc[
        pd.to_numeric(observations_rebased_to_2010["report_year"], errors="coerce")
        >= start_year
    ]

    filtered = filtered[
        ["report_period", "consumer_price_index", "country", "base_year"]
    ]

    return filtered


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

    print("start_year: ", str(start_month[:4]))

    # Initialize a results dataframe
    results_df = pd.DataFrame(
        {"report_period": [], "consumer_price_index": [], "country": []}
    )

    # For each country
    for country in countries:
        print("pulling data for current country: ", country)
        # Pull the CPI data
        curr_country_infl_df = pull_quarterly_cpi_data_from_imf(country, start_month)

        # Append it to the results dataframe
        results_df = pd.concat([results_df, curr_country_infl_df])

        # Sleep between each call since the API is rate limited
        time.sleep(WAIT_TIME_SECONDS)

    # Add a column with the current date
    results_df["last_updated"] = curr_date

    # Write the final results_df to GCS bucket
    final_results_fpath = GCS_BUCKET + RESULTS_FPATH % (curr_date)
    print("final_results_fpath: ", final_results_fpath)
    results_df.to_csv(final_results_fpath, index=False)

    # Load the data from GCS into the table - do a full
    client = bigquery.Client(TARGET_PROJECT)
    load_csv_to_inflation_v1_job = client.load_table_from_uri(
        final_results_fpath,
        TARGET_TABLE,
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_NEVER",
            write_disposition="WRITE_TRUNCATE",
            schema=[
                {"name": "report_period", "type": "STRING", "mode": "NULLABLE"},
                {"name": "consumer_price_index", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "country", "type": "STRING", "mode": "NULLABLE"},
                {"name": "base_year", "type": "STRING", "mode": "NULLABLE"},
                {"name": "last_updated", "type": "DATE", "mode": "NULLABLE"},
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        ),
    )

    load_csv_to_inflation_v1_job.result()


if __name__ == "__main__":
    main()
