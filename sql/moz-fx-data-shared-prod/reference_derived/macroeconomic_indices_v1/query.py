#!/usr/bin/env python

"""Retrieve and load daily stock index and foreign exchange data."""

import logging
import urllib
from argparse import ArgumentParser
from datetime import date, datetime
from typing import Any

import google.auth  # type: ignore
import requests
from google.cloud import bigquery

BASE_URL = "https://financialmodelingprep.com/api/v3"
INDEX_TICKERS = [
    "^DJI",  # Dow Jones Industrial Average
    "^GSPC",  # SNP - SNP Real Time Price. Currency in USD
    "^IXIC",  # Nasdaq GIDS - Nasdaq GIDS Real Time Price. Currency in USD
]
FOREX_TICKERS = [
    "EURUSD=X",  # Euro to USD exchange rate
    "GBPUSD=X",  # GB pound to USD exchange rate
]


parser = ArgumentParser(description=__doc__)
parser.add_argument("--api-key", required=True)
parser.add_argument("--destination_project", default="moz-fx-data-shared-prod")
parser.add_argument("--destination_dataset", default="reference_derived")
parser.add_argument("--destination_table", default="macroeconomic_indices_v1")
parser.add_argument("--market-date")
parser.add_argument("--backfill", action="store_true")
parser.add_argument("--start-date")
parser.add_argument("--end-date")


def get_index_ticker(
    api_key: str, ticker: str, start_date: date, end_date: date
) -> dict[str, Any]:
    """Get historical data for a stock index ticker from the API."""
    encoded_ticker = urllib.parse.quote(ticker)
    url = f"{BASE_URL}/historical-price-full/index/{encoded_ticker}"

    response = requests.get(
        url,
        params={
            "apikey": api_key,
            "from": start_date.strftime("%Y-%m-%d"),
            "to": end_date.strftime("%Y-%m-%d"),
        },
    )
    return response.json()


def get_forex_ticker(
    api_key: str, ticker: str, start_date: date, end_date: date
) -> dict[str, Any]:
    """Get historical data for a foreign exchange ticker from the API."""
    encoded_ticker = urllib.parse.quote(ticker)
    url = f"{BASE_URL}/historical-price-full/forex/{encoded_ticker}"

    response = requests.get(
        url,
        params={
            "apikey": api_key,
            "from": start_date.strftime("%Y-%m-%d"),
            "to": end_date.strftime("%Y-%m-%d"),
        },
    )
    return response.json()


def get_macro_data(api_key: str, start_date: date, end_date: date) -> list[dict]:
    """Pull macroeconomic data from start_date to end_date (inclusive)."""
    logging.info(f"Downloading market data from {start_date} to {end_date}")
    macro_data: list[dict[str, Any]] = []

    for ticker in INDEX_TICKERS:
        ticker_data = get_index_ticker(
            api_key=api_key,
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
        )
        # Exchanges are closed on weekends and some holidays, so these calls
        # could return nothing
        if ticker_data:
            macro_data.append(ticker_data)

    for ticker in FOREX_TICKERS:
        ticker_data = get_forex_ticker(
            api_key=api_key,
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
        )
        # Exchanges are closed on weekends and some holidays, so these calls
        # could return nothing
        if ticker_data:
            macro_data.append(ticker_data)

    logging.info("Market data successfully downloaded")
    return [
        {
            "symbol": row["symbol"],
            "market_date": day["date"],
            "open": day["open"],
            "close": day["close"],
            "adj_close": day["adjClose"],
            "high": day["high"],
            "low": day["low"],
            "volume": day["volume"],
        }
        for row in macro_data
        for day in row["historical"]
    ]


def load_data_to_bq(destination_table: str, macro_data: list[dict]):
    """Load downloaded data to BQ table."""
    credentials, project = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/bigquery",
        ]
    )
    client = bigquery.Client(credentials=credentials, project=project)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("market_date", "DATE"),
            bigquery.SchemaField("open", "NUMERIC"),
            bigquery.SchemaField("close", "NUMERIC"),
            bigquery.SchemaField("adj_close", "NUMERIC"),
            bigquery.SchemaField("high", "NUMERIC"),
            bigquery.SchemaField("low", "NUMERIC"),
            bigquery.SchemaField("volume", "INTEGER"),
        ],
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = client.load_table_from_json(
        macro_data, destination_table, job_config=job_config
    )
    job.result()


def main():
    """Either get and load a single day of data or backfill the entire table."""
    args = parser.parse_args()
    assert args.api_key, "You must provide an API key"

    if args.backfill:
        assert (
            args.start_date is not None and args.end_date is not None
        ), "You must provide a start and end date to backfill"
        assert args.market_date is None, "market date does not apply to backfills"
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        macro_data = get_macro_data(args.api_key, start_date, end_date)
        destination_table = f"{args.destination_project}.{args.destination_dataset}.{args.destination_table}"

    else:
        assert (
            args.start_date is None and args.end_date is None
        ), "Start and end date only apply to backfills"
        assert (
            args.market_date is not None
        ), "You must provide a market date or --backfill + start and end date"
        market_date = end_date = datetime.strptime(args.market_date, "%Y-%m-%d").date()
        macro_data = get_macro_data(args.api_key, market_date, market_date)
        partition_str = market_date.strftime("%Y%m%d")
        destination_table = f"{args.destination_project}.{args.destination_dataset}.{args.destination_table}${partition_str}"

    # On a weekend or holiday we might not get any data at all
    if macro_data:
        load_data_to_bq(
            destination_table=destination_table,
            macro_data=macro_data,
        )
        logging.info(f"Loaded data to {destination_table}")
    else:
        logging.info("No data returned")


if __name__ == "__main__":
    main()
