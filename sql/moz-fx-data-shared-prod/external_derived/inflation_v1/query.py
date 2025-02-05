# Load libraries
from datetime import datetime, timedelta
import pandas as pd
import requests
from argparse import ArgumentParser
from google.cloud import bigquery
import time

#Define countries to pull data for
countries = ['US'] #, 'DE', 'FR', 'GB']

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
  print('api_url')
  print(api_url)


  response = requests.get(api_url)
  inflation_data = response.json()

  print('inflation_data')
  print(inflation_data)

  series = inflation_data.get('CompactData', {}).get('DataSet', {}).get('Series', None)
  if series is None:
    raise KeyError("Expected 'Series' key not found in the API response.")
  
  print('series')
  print(series)
  
  observations = series.get('Obs', [])
  if not observations:
    raise ValueError("No observations found for the specified parameters.")
  
  print('observations')
  print(observations)

  #Now, convert the observations into a data frame
  #observations_df = pd.DataFrame(observations)
  #print('observations_df')
  #print(observations_df)



def main():
    """Call the API, save data to GCS, load to BQ staging, delete & load to BQ gold"""
    #parser = ArgumentParser(description=__doc__)
    #parser.add_argument("--date", required=True)

    #Calculate start / end period based on submission date

    #For each country
    for country in countries: 
      #Pull the CPI data
      pull_monthly_cpi_data_from_imf(country, '2023-10', '2023-12') #start_month, end_month
      
      #Save the data to BQ

      #sleep for 10 seconds between each call since the API is rate limited
      #time.sleep(2)


if __name__ == "__main__":
  main()
