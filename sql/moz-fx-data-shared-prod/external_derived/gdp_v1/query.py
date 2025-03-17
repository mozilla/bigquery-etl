# Load libraries
from datetime import datetime, timedelta
import pandas as pd
import requests
from google.cloud import bigquery

# Set variables
countries = [
    "AUS",
    "BEL",
    "BOL",
    "BRA",
    "BLZ",
    "CAN",
    "CHE",
    "CHN",
    "COL",
    "CRI",
    "DEU",
    "DNK",
    "ESP",
    "FIN",
    "FRA",
    "GBR",
    "GRC",
    "GRL",
    "IDN",
    "IND",
    "IRL",
    "ITA",
    "ISL",
    "JPN",
    "KEN",
    "MEX",
    "NGA",
    "NLD",
    "NOR",
    "POL",
    "SRB",
    "SWE",
    "SGP",
    "TUR" "USA",
]
LOOKBACK_YEARS = 5
TARGET_PROJECT = "moz-fx-data-shared-prod"
TARGET_TABLE = "moz-fx-data-shared-prod.external_derived.gdp_v1"
GCS_BUCKET = "gs://moz-fx-data-prod-external-data/"
GCS_BUCKET_NO_GS = "moz-fx-data-prod-external-data"
RESULTS_FPATH = "IMF_GDP/imf_gdp_data_%s.csv"


# Define function to pull CPI data
def pull_yearly_gdp_data_from_imf(country_query, start_year, end_year):
    """
    Inputs:
      A country code - ISO 3 letter
      Start Year
      End Year
    Output:
      GDP for that country
    """

    base_url = "http://api.worldbank.org/v2/country"
    end_url = f"/{country_query}/indicator/NY.GDP.MKTP.CD?format=json&date={start_year}:{end_year}&per_page=10000"

    api_url = base_url + end_url

    response = requests.get(api_url, timeout=10)
    gdp_data = response.json()
    gdp_data = gdp_data[1]

    # Initialize a results data frame
    results_df = pd.DataFrame(
        {
            "gdp_type": [],
            "gdp_unit_of_measurement": [],
            "gdp_country_code": [],
            "gdp_country_name": [],
            "gdp_country_code_iso3": [],
            "gdp_year": [],
            "gdp_value": [],
        }
    )

    # Put the data into a dataframe
    for entry in gdp_data:
        gdp_type = entry["indicator"]["id"]
        gdp_measurement_type = entry["indicator"]["value"]
        gdp_country_code_2_letter = entry["country"]["id"]
        gdp_country_name = entry["country"]["value"]
        gdp_country_code_3_letter = entry["countryiso3code"]
        gdp_year = entry["date"]
        gdp_value = entry["value"]

        entry_df = pd.DataFrame(
            {
                "gdp_type": [gdp_type],
                "gdp_unit_of_measurement": [gdp_measurement_type],
                "gdp_country_code": [gdp_country_code_2_letter],
                "gdp_country_name": [gdp_country_name],
                "gdp_country_code_iso3": [gdp_country_code_3_letter],
                "gdp_year": [gdp_year],
                "gdp_value": [gdp_value],
            }
        )

        results_df = pd.concat([results_df, entry_df])

    return results_df


if __name__ == "__main__":
    # Create the country query
    country_query = ";".join(countries)

    # Get current date
    today = datetime.today()
    curr_date = today.strftime("%Y-%m-%d")
    print("curr_date")
    print(curr_date)

    # Calculate start year
    start_year_stg = today - timedelta(days=LOOKBACK_YEARS * 365)
    start_year = start_year_stg.strftime("%Y")
    print("start_year: ", start_year)

    # Calculate end year as the prior year
    end_year = int(today.strftime("%Y")) - 1
    print("end_year")
    print(end_year)

    # Pull the data
    gdp = pull_yearly_gdp_data_from_imf(country_query, start_year, end_year)

    # Add a column with the current date
    gdp["last_updated"] = curr_date

    print("gdp")
    print(gdp)

    # Write the final results_df to GCS bucket
    final_results_fpath = GCS_BUCKET + RESULTS_FPATH % (curr_date)
    print("final_results_fpath: ", final_results_fpath)
    gdp.to_csv(final_results_fpath, index=False)

    # Load to BQ  - write/truncate
    client = bigquery.Client(TARGET_PROJECT)
    load_csv_to_gdp_v1_job = client.load_table_from_uri(
        final_results_fpath,
        TARGET_TABLE,
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_NEVER",
            write_disposition="WRITE_TRUNCATE",
            schema=[
                {"name": "gdp_type", "type": "STRING", "mode": "NULLABLE"},
                {
                    "name": "gdp_unit_of_measurement",
                    "type": "STRING",
                    "mode": "NULLABLE",
                },
                {"name": "gdp_country_code", "type": "STRING", "mode": "NULLABLE"},
                {"name": "gdp_country_name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "gdp_country_code_iso3", "type": "STRING", "mode": "NULLABLE"},
                {"name": "gdp_year", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "gdp_value", "type": "BIGNUMERIC", "mode": "NULLABLE"},
                {"name": "last_updated", "type": "DATE", "mode": "NULLABLE"},
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        ),
    )

    load_csv_to_gdp_v1_job.result()
