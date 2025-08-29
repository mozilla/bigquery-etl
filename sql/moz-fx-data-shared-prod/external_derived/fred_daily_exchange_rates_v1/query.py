# Load libraries
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
import os
from fredapi import Fred
from argparse import ArgumentParser

# Define inputs
# Series ID, From Currency, To Currency, Reverse Direction Before Storing in BQ

# Note: we are doing the "reverse" step because the series are only available in certain directions
# but for ease of use, we'd like them all to be from different countries to the USD when we store in BQ
exchange_rates = {
    "AU": ["DEXUSAL", "USD", "AUD", True],  # USD to AUD
    "CA": ["DEXCAUS", "CAD", "USD", False],  # CAD to USD
    "CH": ["DEXSZUS", "CHF", "USD", False],  # CHF to USD
    "JP": ["DEXJPUS", "JPY", "USD", False],  # JPY to USD
    "GB": ["DEXUSUK", "USD", "GBP", True],  # USD to GBP
    "EU": ["DEXUSEU", "USD", "EUR", True],  # USD to EUR
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
    ).astype(
        {
            "submission_date": "object",  # store Python datetime.date here
            "conversion_type": "string",
            "exchange_rate": "float",
        }
    )

    # We want to always run for 4 days ago to give time for data to load
    logical_dag_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    execution_date = logical_dag_date - timedelta(days=4)
    print(
        f"Pulling data for execution_date: {execution_date} for logical dag date {logical_dag_date}"
    )

    for k, v in exchange_rates.items():
        country = k
        series_id, from_currency, to_currency, needs_to_be_reversed = v

        # Fetch the series for the date window
        series = fred.get_series(
            series_id=series_id,
            observation_start=execution_date,
            observation_end=execution_date,
        )

        # Convert Series -> DataFrame with your schema
        new_data_df = series.reset_index()
        # fredapi typically names the columns ['index', 0] after reset_index() – rename them:
        new_data_df.columns = ["submission_date", "exchange_rate"]

        # Keep only rows with data
        new_data_df = new_data_df.dropna(subset=["exchange_rate"])

        # Some days will be empty (i.e. weekends & holidays)
        # Skip so they don't concatenate
        if new_data_df.empty:
            continue

        # Add conversion_type column
        if needs_to_be_reversed is False:
            conversion_type = f"{from_currency}_to_{to_currency}"
            new_data_df["exchange_rate"] = new_data_df["exchange_rate"]
        else:
            conversion_type = f"{to_currency}_to_{from_currency}"
            new_data_df["exchange_rate"] = 1 / new_data_df["exchange_rate"]
        new_data_df["conversion_type"] = conversion_type

        # Reorder columns to match results_df
        new_data_df = new_data_df[
            ["submission_date", "conversion_type", "exchange_rate"]
        ]

        new_data_df = new_data_df.astype(
            {
                "submission_date": "object",
                "conversion_type": "string",
                "exchange_rate": "float",
            }
        )

        # Append
        results_df = pd.concat([results_df, new_data_df], ignore_index=True)

    # Convert from datetime to date
    results_df["submission_date"] = pd.to_datetime(
        results_df["submission_date"]
    ).dt.date

    print(f"Fetched {len(results_df)} rows")

    # Open a BQ clieent
    client = bigquery.Client()

    # Format partition decorator as YYYYMMDD
    partition = execution_date.strftime("%Y%m%d")

    # Point the load at just that partition
    partitioned_table_id = f"{table_id}${partition}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField(
                name="submission_date",
                field_type="DATE",
                mode="NULLABLE",
                description="Submission Date",
            ),
            bigquery.SchemaField(
                name="conversion_type",
                field_type="STRING",
                mode="NULLABLE",
                description="Conversion Type - From Currency, To Currency",
            ),
            bigquery.SchemaField(
                name="exchange_rate",
                field_type="FLOAT",
                mode="NULLABLE",
                description="Exchange Rate",
            ),
        ],
        autodetect=False,
    )

    load_job = client.load_table_from_dataframe(
        results_df,
        partitioned_table_id,
        job_config=job_config,
    )
    load_job.result()  # waits for completion

    print(f"Replaced partition {execution_date} in {table_id}")
