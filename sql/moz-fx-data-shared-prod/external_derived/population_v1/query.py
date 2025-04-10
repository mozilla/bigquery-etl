# Load packages
import os
from argparse import ArgumentParser
from datetime import datetime

import pandas as pd
import requests
from google.cloud import bigquery

# Set variables
INDICATOR_ID_OF_INTEREST = 49  # Total population by sex"
LOC_IDS_OF_INTEREST = [
    124,  # Canada
    276,  # Germany
    380,  # Italy
    372,  # Ireland
    356,  # India
    392,  # Japan
    484,  # Mexico
    616,  # Poland
    752,  # Sweden,
    756,  # Switzerland
    792,  # Turkey
    246,  # Finland
    826,  # UK
    250,  # FR
    840,  # US
    36,  # Australia
    56,  # Belgium
    68,  # Bolivia
    76,  # Brazil
    84,  # Belize
    156,  # China
    170,  # Colombia
    188,  # Costa Rica
    208,  # Denmark
    304,  # Greenland
    300,  # Greece
    352,  # Iceland
    360,  # Indonesia
    404,  # Kenya
    528,  # Netherlands
    566,  # Nigeria
    578,  # Norway
    688,  # Serbia
    702,  # Singapore
    724, # Spain
]
TARGET_PROJECT = "moz-fx-data-shared-prod"
TARGET_TABLE = "moz-fx-data-shared-prod.external_derived.population_v1"
GCS_BUCKET = "gs://moz-fx-data-prod-external-data/"

# Pull bearer token from Google Secret Manager
bearer_token = os.getenv("UN_POPULATION_BEARER_TOKEN")

def fetch_data(url, hdr, pyld, timeout_limit):
    """Inputs: URL, Header, Payload, Timeout Limit (seconds)
    Output: Raises an error if an issue arises during the fetch"""
    try:
        response = requests.get(url, headers=hdr, data=pyld, timeout=timeout_limit)
        response.raise_for_status()  # Raises an HTTPError for 4xx and 5xx status codes
        return response.json()  # or response.text if expecting plain text
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")  # Handle HTTP errors (4xx and 5xx)
        raise  # Re-raise the error if needed
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to the server.")
        raise
    except requests.exceptions.Timeout:
        print("Error: The request timed out.")
        raise
    except requests.exceptions.RequestException as err:
        print(f"An error occurred: {err}")
        raise


# Define function to retrieve population data from United Nations API
def pull_population_data(year_to_pull_data_for, location_id, indicator_id):
    """Input: year to pull data for, location to pull data for, indicator_id (type of data you want)
    Output: JSON with population data
    Results are paginated, so if there is a non-null "nextPage", need to submit another request to get next page of results
    """
    url = f"https://population.un.org/dataportalapi/api/v1/data/indicators/{indicator_id}/locations/{location_id}/start/{year_to_pull_data_for}/end/{year_to_pull_data_for}?pagingInHeader=false&format=json"
    headers = {"Authorization": f"Bearer {bearer_token}"}
    payload = {}
    results = fetch_data(url, hdr=headers, pyld=payload, timeout_limit=10)

    # Initialize the dataframe with the first set of results
    results_df = pd.DataFrame(results["data"])
    # Code is currently only built to handle 1 page of results, so error out if there is more than 1
    if results["nextPage"] is not None:
        raise Exception(
            "More than 1 page of results provided; code only built to handle 1 page"
        )

    # Rename the columns to match the columns in schema.yaml
    results_df = results_df.rename(
        columns={
            "locationId": "location_id",
            "iso3": "iso3_country_code",
            "iso2": "iso2_country_code",
            "locationTypeId": "location_type_id",
            "indicatorId": "indicator_id",
            "indicatorDisplayName": "indicator_display_name",
            "sourceId": "source_id",
            "variantId": "variant_id",
            "variantShortName": "variant_short_name",
            "variantLabel": "variant_label",
            "timeId": "time_id",
            "timeLabel": "time_label",
            "timeMid": "time_mid",
            "categoryId": "category_id",
            "estimateTypeId": "estimate_type_id",
            "estimateType": "estimate_type",
            "estimateMethodId": "estimate_method_id",
            "estimateMethod": "estimate_method",
            "sexId": "sex_id",
            "ageId": "age_id",
            "ageLabel": "age_label",
            "ageStart": "age_start",
            "ageEnd": "age_end",
            "ageMid": "age_mid",
        }
    )

    return results_df


def main():
    """Call the API, save data to GCS, delete any data already in table for same year, then load to BQ table"""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    logical_dag_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    logical_dag_date_string = logical_dag_date.strftime("%Y-%m-%d")

    # Calculate current date
    today = datetime.today()
    curr_date = today.strftime("%Y-%m-%d")

    # Calculate year of interest from the DAG run date
    # DAG runs 1x a year, so this gets the year from the DAG logical date
    year_of_interest = logical_dag_date.strftime("%Y")
    year_of_interest = str(int(year_of_interest) + 1)
    print(f"Pulling data for year: {year_of_interest}")

    # Initialize an empty data frame which we will append all results to
    full_results_df = pd.DataFrame(
        {
            "location_id": [],
            "location": [],
            "iso3_country_code": [],
            "iso2_country_code": [],
            "location_type_id": [],
            "indicator_id": [],
            "indicator": [],
            "indicator_display_name": [],
            "source_id": [],
            "source": [],
            "revision": [],
            "variant_id": [],
            "variant": [],
            "variant_short_name": [],
            "variant_label": [],
            "time_id": [],
            "time_label": [],
            "time_mid": [],
            "category_id": [],
            "category": [],
            "estimate_type_id": [],
            "estimate_type": [],
            "estimate_method_id": [],
            "estimate_method": [],
            "sex_id": [],
            "sex": [],
            "age_id": [],
            "age_label": [],
            "age_start": [],
            "age_end": [],
            "age_mid": [],
            "value": [],
            "last_updated": [],
        }
    )

    # For each location
    for loc_id in LOC_IDS_OF_INTEREST:
        # Get the population data as a dataframe
        population_data = pull_population_data(
            year_of_interest, loc_id, INDICATOR_ID_OF_INTEREST
        )

        # Append the new data to the full_results_df
        full_results_df = pd.concat([full_results_df, population_data])

    # Enforce data types
    full_results_df["location_id"] = full_results_df["location_id"].astype(int)
    full_results_df["location_type_id"] = full_results_df["location_type_id"].astype(
        int
    )
    full_results_df["indicator_id"] = full_results_df["indicator_id"].astype(int)
    full_results_df["source_id"] = full_results_df["source_id"].astype(int)
    full_results_df["revision"] = full_results_df["revision"].astype(int)
    full_results_df["variant_id"] = full_results_df["variant_id"].astype(int)
    full_results_df["time_id"] = full_results_df["time_id"].astype(int)
    full_results_df["category_id"] = full_results_df["category_id"].astype(int)
    full_results_df["estimate_type_id"] = full_results_df["estimate_type_id"].astype(
        int
    )
    full_results_df["estimate_method_id"] = full_results_df[
        "estimate_method_id"
    ].astype(int)
    full_results_df["sex_id"] = full_results_df["sex_id"].astype(int)
    full_results_df["age_id"] = full_results_df["age_id"].astype(int)
    full_results_df["age_start"] = full_results_df["age_start"].astype(int)
    full_results_df["age_end"] = full_results_df["age_end"].astype(int)

    # Add last updated date
    full_results_df["last_updated"] = curr_date

    # Calculate GCS filepath to write to and then write CSV to that filepath
    fpath = (
        GCS_BUCKET
        + f"UN_Population_Data/pop_data_year_{year_of_interest}_as_of_{logical_dag_date_string}.csv"
    )
    full_results_df.to_csv(fpath, index=False)

    # Open a connection to BQ
    client = bigquery.Client(TARGET_PROJECT)

    # Delete any data already in table for same year so we don't end up with duplicates
    delete_query = f"""DELETE FROM `moz-fx-data-shared-prod.external_derived.population_v1`
  WHERE time_label = '{year_of_interest}'"""
    del_job = client.query(delete_query)
    del_job.result()

    # Load data from GCS to BQ table - appending to what is already there
    load_csv_to_gcp_job = client.load_table_from_uri(
        fpath,
        TARGET_TABLE,
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_NEVER",
            write_disposition="WRITE_APPEND",
            schema=[
                {"name": "location_id", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "location", "type": "STRING", "mode": "NULLABLE"},
                {"name": "iso3_country_code", "type": "STRING", "mode": "NULLABLE"},
                {"name": "iso2_country_code", "type": "STRING", "mode": "NULLABLE"},
                {"name": "location_type_id", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "indicator_id", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "indicator", "type": "STRING", "mode": "NULLABLE"},
                {
                    "name": "indicator_display_name",
                    "type": "STRING",
                    "mode": "NULLABLE",
                },
                {"name": "source_id", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "source", "type": "STRING", "mode": "NULLABLE"},
                {"name": "revision", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "variant_id", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "variant", "type": "STRING", "mode": "NULLABLE"},
                {"name": "variant_short_name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "variant_label", "type": "STRING", "mode": "NULLABLE"},
                {"name": "time_id", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "time_label", "type": "STRING", "mode": "NULLABLE"},
                {"name": "time_mid", "type": "STRING", "mode": "NULLABLE"},
                {"name": "category_id", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "category", "type": "STRING", "mode": "NULLABLE"},
                {"name": "estimate_type_id", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "estimate_type", "type": "STRING", "mode": "NULLABLE"},
                {"name": "estimate_method_id", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "estimate_method", "type": "STRING", "mode": "NULLABLE"},
                {"name": "sex_id", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "sex", "type": "STRING", "mode": "NULLABLE"},
                {"name": "age_id", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "age_label", "type": "STRING", "mode": "NULLABLE"},
                {"name": "age_start", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "age_end", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "age_mid", "type": "NUMERIC", "mode": "NULLABLE"},
                {"name": "value", "type": "NUMERIC", "mode": "NULLABLE"},
                {"name": "last_updated", "type": "DATE", "mode": "NULLABLE"},
            ],
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
        ),
    )

    load_csv_to_gcp_job.result()


if __name__ == "__main__":
    main()
