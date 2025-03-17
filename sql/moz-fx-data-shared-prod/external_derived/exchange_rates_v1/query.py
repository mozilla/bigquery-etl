# Load libraries
from datetime import datetime, timedelta
import time
import pandas as pd
import requests
from google.cloud import bigquery


# Define inputs
country_codes = ["CA", "GB", "JP", "PL"]
LOOKBACK_YEARS = 5
TARGET_PROJECT = "moz-fx-data-shared-prod"
TARGET_TABLE = "moz-fx-data-shared-prod.external_derived.exchange_rates_v1"
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
    dataset_key = f"CompactData/IFS/{frequency}.{country_code}.ENDA_XDC_USD_RATE"

    # Define query parameters
    params = {}
    if start_period:
        params["startPeriod"] = start_period
    if end_period:
        params["endPeriod"] = end_period

    try:
        # Fetch data from the API
        data_url = f"{base_url}{dataset_key}"
        response = requests.get(data_url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        # Extract the 'Series' element
        series = data.get("CompactData", {}).get("DataSet", {}).get("Series", None)
        if series is None:
            raise KeyError("Expected 'Series' key not found in the API response.")

        # Extract observations
        observations = series.get("Obs", [])
        if not observations:
            raise ValueError("No observations found for the specified parameters.")

        # Convert observations to a DataFrame
        df = pd.DataFrame(observations)
        df.rename(
            columns={"@TIME_PERIOD": "report_period", "@OBS_VALUE": "exchange_rate"},
            inplace=True,
        )
        df["frequency"] = frequency
        df["country_code"] = country_code

        return df

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching the data: {e}")
    except KeyError as e:
        print(f"An error occurred while accessing the JSON structure: {e}")
    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


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
    base_url = "http://dataservices.imf.org/REST/SDMX_JSON.svc/"
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
