"""Retrieves survey data from Alchemer and stores it in BigQuery."""

from bigquery_etl.alchemer.survey import main

if __name__ == "__main__":
    main()
