# Load libraries
from datetime import datetime, timedelta
import time
import pandas as pd
import requests
from google.cloud import bigquery

#
countries = ['US', 'DE', 'MX', 'GB']

#Load token from GSM


def pull_population_data(?, ?, ?):
    """ """

    return pop_df

if __name__ == "__main__":
    #Calculate todays date
    today = datetime.today()
    curr_date = today.strftime("%Y-%m-%d")
    print("curr_date")
    print(curr_date)


    population_data = pull_population_data()

    #Load to GCS

    #Load to BQ
