# Load libraries
from datetime import datetime, timedelta
import pandas as pd
import requests
import time
from argparse import ArgumentParser
from google.cloud import bigquery
from google.cloud import storage

# Set variables
countries = ['AUS', 'CAN', 'CHE', 'DEU', 'ESP', 'FRA', 'GBR', 'ITA', 'JPN', 'POL', 'USA']
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


    print('gdp_data')
    print(gdp_data)
    #TEMP - preview data



if __name__ == "__main__":
    #Create the country query
    country_query = ";".join(countries)

    #Get current date
    today = datetime.today()
    curr_date = today.strftime("%Y-%m-%d")
    print("curr_date")
    print(curr_date)

    #Calculate start year
    start_year_stg = today - timedelta(days=LOOKBACK_YEARS * 365)
    start_year = start_year_stg.replace(day=1).strftime("%Y-%m")
    print("start_year: ", start_year)

    #Calculate end year as the prior year
    end_year = int(today.strftime("%Y")) - 1
    print('end_year')
    print(end_year)

    #Pull the data
    pull_yearly_gdp_data_from_imf(country_query,  start_year, end_year)



###### SCRATCHWORK BELOW ###
# def fetch_worldbank_gdp_data(indicator, country_codes, start_year, end_year):
#     """
#     Fetch GDP data from the World Bank API for specified countries and years.

#     :param indicator: World Bank indicator code (e.g., "NY.GDP.MKTP.CD" for nominal GDP).
#     :param country_codes: List of country codes (ISO Alpha-3) (e.g., ['USA', 'JPN', 'AUS']).
#     :param start_year: Start year of the data range (e.g., 2021).
#     :param end_year: End year of the data range (e.g., 2024).
#     :return: DataFrame containing GDP data.
#     """
#     # World Bank API Base URL
#     WB_BASE_URL = "http://api.worldbank.org/v2/country"

#     # Validate country codes and split into batches if needed
#     if len(country_codes) > 50:
#         raise ValueError("Too many countries in one request. Limit to 50 per request.")

#     # Construct query
#     country_query = ";".join(country_codes)
#     url = f"{WB_BASE_URL}/{country_query}/indicator/{indicator}?format=json&date={start_year}:{end_year}&per_page=10000"

#     try:
#         response = requests.get(url, timeout=30)  # Set timeout to 30 seconds
#         response.raise_for_status()  # Raise an error for bad responses

#         # Parse the response
#         data = response.json()
#         if len(data) < 2:
#             raise Exception("No data available for the specified parameters.")

#         gdp_data = data[1]
#         processed_data = []

#         for entry in gdp_data:
#             if "value" in entry and entry["value"] is not None:
#                 processed_data.append({
#                     "Country Code": entry["country"]["id"],
#                     "Country Name": entry["country"]["value"],
#                     "Year": entry["date"],
#                     "GDP (Nominal USD)": entry["value"]
#                 })

#         # Create and return DataFrame
#         return pd.DataFrame(processed_data)

#     except requests.exceptions.Timeout:
#         raise Exception("The request timed out. Please throttle your requests or try again later.")
#     except requests.exceptions.RequestException as e:
#         raise Exception(f"Request error: {e}")
#     except KeyError:
#         raise Exception("Invalid response format or no data available for the specified parameters.")


# # Example usage
# if __name__ == "__main__":
#     WB_INDICATOR = "NY.GDP.MKTP.CD"  # World Bank indicator for nominal GDP in USD
#     test_country_codes = ['AUS', 'CAN', 'CHE', 'DEU', 'ESP', 'FRA', 'GBR', 'ITA', 'JPN', 'POL', 'USA']  # ISO Alpha-3 codes
#     test_start_year = 2021
#     test_end_year = 2024

#     try:
#         # Fetch GDP data
#         gdp_data = fetch_worldbank_gdp_data(WB_INDICATOR, test_country_codes, test_start_year, test_end_year)
#         print(gdp_data.head())
#         gdp_data.to_csv("worldbank_gdp_data.csv", index=False)  # Save results if needed
#     except Exception as e:
#         print(f"Error: {e}")