#!/usr/bin/env python

"""Import exchange rates from Oanda."""

import datetime
import json
from argparse import ArgumentParser
from dataclasses import dataclass
from decimal import Decimal

import requests
from google.cloud import bigquery


@dataclass(frozen=True)
class Quote:
    """Exchange rate quote."""

    date: datetime.date
    base_currency: str
    quote_currency: str
    price_type: str
    price: Decimal


parser = ArgumentParser(__doc__)
parser.add_argument("--start-date", required=True)
parser.add_argument("--end-date", required=True)
parser.add_argument("--table", required=True)
parser.add_argument(
    "--write-disposition",
    choices=[
        bigquery.WriteDisposition.WRITE_EMPTY,
        bigquery.WriteDisposition.WRITE_TRUNCATE,
        bigquery.WriteDisposition.WRITE_APPEND,
    ],
)
parser.add_argument(
    "--base-currencies",
    "--base-currency",
    nargs="+",
    required=True,
)
parser.add_argument("--quote-currency", default="USD")
parser.add_argument("--price", default="mid", choices=["bid", "ask", "mid"])
args = parser.parse_args()

quotes = set()
for base_currency in args.base_currencies:
    response = requests.get(
        "https://fxds-hcc.oanda.com/api/data/update/",
        params={
            "source": "OANDA",
            "adjustment": "0",
            "base_currency": base_currency,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "period": "daily",
            "price": args.price,
            "view": "graph",
            "quote_currency_0": args.quote_currency,
        },
    )
    try:
        response.raise_for_status()
    except Exception:
        print(response.request)
        raise
    try:
        quotes |= {
            Quote(
                date=datetime.datetime.utcfromtimestamp(millis / 1000).date(),
                quote_currency=currency_pair["quoteCurrency"],
                base_currency=currency_pair["baseCurrency"],
                price_type=currency_pair["type"],
                price=round(Decimal(price), 9),
            )
            for currency_pair in response.json()["widget"]
            for millis, price in currency_pair["data"]
        }
    except Exception:
        print("invalid response:")
        print(response.text)
        raise

if not quotes:
    print("No exchange rates found.")
else:
    json_rows = [
        {
            "date": quote.date.isoformat(),
            "quote_currency": quote.quote_currency,
            "base_currency": quote.base_currency,
            "price_type": quote.price_type,
            "price": str(quote.price),
        }
        for quote in sorted(quotes, key=lambda q: q.date)
    ]
    print(*map(json.dumps, json_rows), sep="\n")
    table = args.table
    if args.start_date == args.end_date and "$" not in table:
        table = f"{table}${args.start_date.replace('-', '')}"
    print(f"writing to {table}")
    write_disposition = args.write_disposition
    if write_disposition is None:
        if "$" in table:
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
    job_config = bigquery.LoadJobConfig(
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        schema=[
            bigquery.SchemaField("date", "DATE", "REQUIRED"),
            bigquery.SchemaField("base_currency", "STRING", "REQUIRED"),
            bigquery.SchemaField("quote_currency", "STRING", "REQUIRED"),
            bigquery.SchemaField("price_type", "STRING", "REQUIRED"),
            bigquery.SchemaField("price", "NUMERIC", "REQUIRED"),
        ],
        time_partitioning=bigquery.TimePartitioning(
            bigquery.TimePartitioningType.DAY, "date"
        ),
        write_disposition=write_disposition,
    )
    client = bigquery.Client()
    job = client.load_table_from_json(json_rows, table, job_config=job_config)
    job.result()
