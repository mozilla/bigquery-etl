# Load libraries
from datetime import datetime, timedelta
import pandas as pd
import requests
from argparse import ArgumentParser
from google.cloud import bigquery
import time

#Define countries to pull data for
countries = ['US', 'DE']


#Define function to pull CPI data 
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

  response = requests.get(api_url)
  data = response.json()


  #TEMP ABOVE 


    # try:
    #     # Fetch data from the API
    #     data_url = f"{base_url}{dataset_key}"
    #     response = requests.get(data_url, params=params)
    #     response.raise_for_status()
    #     data = response.json()

    #     # Extract the 'Series' element
    #     series = data.get('CompactData', {}).get('DataSet', {}).get('Series', None)
    #     if series is None:
    #         raise KeyError("Expected 'Series' key not found in the API response.")
        
    #     # Extract observations
    #     observations = series.get('Obs', [])
    #     if not observations:
    #         raise ValueError("No observations found for the specified parameters.")
        
    #     # Extract base year if available
    #     base_year = series.get('@BASE_YEAR', "Unknown Base Year")

    #     # Convert observations to a DataFrame
    #     df = pd.DataFrame(observations)
    #     df.rename(columns={'@TIME_PERIOD': 'Date', '@OBS_VALUE': 'Value'}, inplace=True)
    #     df['Date'] = pd.to_datetime(df['Date'])
    #     df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
    #     df['Frequency'] = frequency
    #     df['Country'] = country_code
    #     df['Base Year'] = base_year
        
    #     return df

    # except requests.exceptions.RequestException as e:
    #     print(f"An error occurred while fetching the data: {e}")
    # except KeyError as e:
    #     print(f"An error occurred while accessing the JSON structure: {e}")
    # except ValueError as e:
    #     print(f"Error: {e}")
    # except Exception as e:
    #     print(f"An unexpected error occurred: {e}")

# Pull data


# Load to table



def main():
    """Call the API, save data to GCS, load to BQ staging, delete & load to BQ gold"""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)

    #Calculate start / end period based on submission date

    #For each country
    for country in countries: 
      #Pull the CPI data
      results = pull_monthly_cpi_data_from_imf(country, start_month, end_month)
      
      #Save the data to BQ

      #sleep for 10 seconds between each call since the API is rate limited
      time.sleep(10)

    

if __name__ == "__main__":
  main()
