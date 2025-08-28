# Load libraries
# from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
import os
from fredapi import Fred
from argparse import ArgumentParser

# Define inputs
exchange_rates = {
    "AU": ["DEXUSAL", "USD", "AUD"],  # USD to AUD
    "CA": ["DEXCAUS", "CAD", "USD"],  # CAD to USD
    "CH": ["DEXSZUS", "CHF", "USD"],  # CHF to USD
    "JP": ["DEXJPUS", "JPY", "USD"],  # JPY to USD
    "GB": ["DEXUSUK", "USD", "GBP"],  # USD to GBP
    "EU": ["DEXUSEU", "USD", "EUR"],  # USD to EUR
}

table_id = "moz-fx-data-shared-prod.external_derived.fred_daily_exchange_rates_v1"

# Get the API key
fred_api_key = os.getenv("FRED_API_KEY")
if not fred_api_key:
    raise RuntimeError("FRED_API_KEY environment variable is not set.")

# Create a Fred client instance
fred = Fred(api_key=fred_api_key)

# Get the data for each exchange we are interested in
if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    results_df = pd.DataFrame(
        columns=["submission_date", "conversion_type", "exchange_rate"]
    )

    # Start date and end date are the same (the logical DAG run date)
    start_date = args.date
    end_date = args.date

    for k, v in exchange_rates.items():
        country = k
        series_id, from_currency, to_currency = v

        # Fetch the series for the date window
        series = fred.get_series(
            series_id=series_id, observation_start=start_date, observation_end=end_date
        )

        # Convert Series -> DataFrame with your schema
        new_data_df = series.reset_index()
        # fredapi typically names the columns ['index', 0] after reset_index() – rename them:
        new_data_df.columns = ["submission_date", "exchange_rate"]

        # Keep only rows with data
        new_data_df = new_data_df.dropna(subset=["exchange_rate"])

        # Add conversion_type column
        conversion_type = f"{from_currency}_to_{to_currency}"
        new_data_df["conversion_type"] = conversion_type

        # Reorder columns to match results_df
        new_data_df = new_data_df[
            ["submission_date", "conversion_type", "exchange_rate"]
        ]

        # Append
        results_df = pd.concat([results_df, new_data_df], ignore_index=True)
        print(f"Fetched {len(new_data_df)} rows for {conversion_type} ({series_id})")

    # Convert from datetime to date
    results_df["submission_date"] = pd.to_datetime(
        results_df["submission_date"]
    ).dt.date

    # Open a BQ clieent
    client = bigquery.Client()

    # If any data exists for the date in the BQ table, delete it
    delete_data_for_date_from_bq = f"""DELETE FROM `moz-fx-data-shared-prod.external_derived.fred_daily_exchange_rates_v1`
    WHERE submission_date = '{args.date}'"""

    del_job = client.query(delete_data_for_date_from_bq)
    del_job.result()
    print("Deleted anything already existing for this date from table")

    # Append the data to the BQ table
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    # Execute the load
    load_job = client.load_table_from_dataframe(
        results_df, table_id, job_config=job_config
    )

    load_job.result()  # Waits for the job to complete

    print(
        "Note - this ETL is expected to only have new data for business days, not weekends or holidays"
    )
    print(f"Loaded {results_df.shape[0]} rows into {table_id}.")
