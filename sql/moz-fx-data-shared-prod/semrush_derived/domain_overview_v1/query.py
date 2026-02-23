"""Fetch Semrush domain_ranks data and load to BigQuery."""

import logging
import os
import sys
from argparse import ArgumentParser

import pandas as pd
import requests
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SEMRUSH_API_KEY = os.environ.get("SEMRUSH_API_KEY")
if not SEMRUSH_API_KEY:
    logging.critical("SEMRUSH_API_KEY environment variable not set")
    sys.exit(1)
API_URL = "https://api.semrush.com/"

DOMAINS = ["firefox.com", "mozilla.org"]

DESKTOP_DATABASES = [
    "us",
    "uk",
    "ca",
    "ru",
    "de",
    "fr",
    "es",
    "it",
    "br",
    "au",
    "ar",
    "be",
    "ch",
    "dk",
    "fi",
    "hk",
    "ie",
    "il",
    "mx",
    "nl",
    "no",
    "pl",
    "se",
    "sg",
    "tr",
    "jp",
    "in",
    "hu",
    "af",
    "al",
    "dz",
    "ao",
    "am",
    "at",
    "az",
    "bh",
    "bd",
    "by",
    "bz",
    "bo",
    "ba",
    "bw",
    "bn",
    "bg",
    "cv",
    "kh",
    "cm",
    "cl",
    "co",
    "cr",
    "hr",
    "cy",
    "cz",
    "cd",
    "do",
    "ec",
    "eg",
    "sv",
    "ee",
    "et",
    "ge",
    "gh",
    "gr",
    "gt",
    "gy",
    "ht",
    "hn",
    "is",
    "id",
    "jm",
    "jo",
    "kz",
    "kw",
    "lv",
    "lb",
    "lt",
    "lu",
    "mg",
    "my",
    "mt",
    "mu",
    "md",
    "mn",
    "me",
    "ma",
    "mz",
    "na",
    "np",
    "nz",
    "ni",
    "ng",
    "om",
    "py",
    "pe",
    "ph",
    "pt",
    "ro",
    "sa",
    "sn",
    "rs",
    "sk",
    "si",
    "za",
    "kr",
    "lk",
    "th",
    "bs",
    "tt",
    "tn",
    "ua",
    "ae",
    "uy",
    "ve",
    "vn",
    "zm",
    "zw",
    "ly",
    "pa",
    "pk",
    "tw",
    "qa",
]

MOBILE_DATABASES = [
    "mobile-us",
    "mobile-uk",
    "mobile-ca",
    "mobile-de",
    "mobile-fr",
    "mobile-es",
    "mobile-it",
    "mobile-br",
    "mobile-au",
    "mobile-dk",
    "mobile-mx",
    "mobile-nl",
    "mobile-se",
    "mobile-tr",
    "mobile-in",
    "mobile-id",
    "mobile-il",
]

DATABASES = DESKTOP_DATABASES + MOBILE_DATABASES

FK_CODES = [
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    19,
    20,
    21,
    22,
    23,
    25,
    27,
    31,
    32,
    34,
    35,
    36,
    37,
    38,
    40,
    41,
    43,
    45,
    47,
    50,
    51,
    52,
]

FP_CODES = [5, 6, 11, 13, 21, 43, 52]

EXPORT_COLUMNS = (
    "Dn,Db,Dt,Rk,Or,Ot,Oc,Ad,At,Ac,"
    "Sr,Srb,Srl,Srn,St,Stb,Sc,"
    + ",".join(f"FK{i}" for i in FK_CODES)
    + ","
    + ",".join(f"FP{i}" for i in FP_CODES)
)

BASE_HEADERS = {
    "Domain",
    "Database",
    "Date",
    "Rank",
    "Organic Keywords",
    "Organic Traffic",
    "Organic Cost",
    "Adwords Keywords",
    "Adwords Traffic",
    "Adwords Cost",
    "SERP Features Positions",
    "SERP Features Positions Branded",
    "SERP Features Positions Lost",
    "SERP Features Positions New",
    "SERP Features Traffic",
    "SERP Features Traffic Branded",
    "SERP Features Traffic Cost",
}


REQUEST_TIMEOUT = 30

SESSION = requests.Session()
RETRIES = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])

SESSION.mount("https://", HTTPAdapter(max_retries=RETRIES))

NON_NUMERIC_COLUMNS = {"submission_date", "domain", "database", "refreshed_at"}


def fetch_domain_data(domain, database="us"):
    """Fetch domain_ranks data from Semrush API."""
    resp = SESSION.get(
        API_URL,
        params={
            "key": SEMRUSH_API_KEY,
            "type": "domain_ranks",
            "domain": domain,
            "database": database,
            "export_columns": EXPORT_COLUMNS,
        },
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.text


def parse_response(response_text):
    """Parse semicolon-delimited response into a dict matching the schema.

    Base columns are mapped by converting headers to snake_case.
    SERP feature columns use fk_/fp_ prefixes. The first occurrence
    of a SERP feature name is FK, the second occurrence is FP.
    """
    lines = response_text.strip().splitlines()
    if len(lines) < 2:
        logging.warning(f"Empty response: {response_text[:200]}")
        return None

    headers = lines[0].split(";")
    values = lines[1].split(";")

    # Use responses own date as submission_date (data is typically one day behind).
    header_map = dict(zip(headers, values))
    response_date = header_map.get("Date", "")
    if not response_date or len(response_date) != 8:
        logging.warning(f"Missing or invalid date in response: {response_date}")
        return None

    row = {
        "submission_date": f"{response_date[:4]}-{response_date[4:6]}-{response_date[6:8]}"
    }
    seen_features = set()

    for header, value in zip(headers, values):
        if header == "Date":
            continue

        col = header.lower().replace(" ", "_")

        if header in BASE_HEADERS:
            row[col] = value
        elif col in seen_features:
            row[f"fp_{col}"] = value
        else:
            seen_features.add(col)
            row[f"fk_{col}"] = value

    return row


def main():
    """Fetch Semrush data for all domains and load to BigQuery."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--project", default="moz-fx-data-shared-prod")
    parser.add_argument("--dataset", default="semrush_derived")
    parser.add_argument("--table", default="domain_overview_v1")
    parser.add_argument(
        "--database",
        nargs="*",
        default=None,
        help="One or more databases to query; omit to query all",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print parsed data instead of loading to BigQuery",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    databases = args.database if args.database else DATABASES

    rows = []
    failures = 0
    for database in databases:
        for domain in DOMAINS:
            try:
                logging.info(f"Fetching {domain} / {database}")
                text = fetch_domain_data(domain, database)
                row = parse_response(text)
                if row:
                    rows.append(row)
                else:
                    logging.info(f"No usable data for {domain} / {database}")
            except requests.RequestException as e:
                failures += 1
                logging.warning(f"Failed {domain} / {database}: {e}")

    if not rows and failures:
        raise RuntimeError(f"No data fetched; {failures} API calls failed.")

    if not rows:
        logging.info("No data for this date, skipping load.")
        return

    df = pd.DataFrame(rows)
    df["submission_date"] = pd.to_datetime(df["submission_date"]).dt.date

    for col in df.columns.difference(NON_NUMERIC_COLUMNS):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop_duplicates(subset=["submission_date", "domain", "database"])
    df["refreshed_at"] = pd.Timestamp.now(tz="UTC")

    if args.dry_run:
        print(df.to_csv())
        logging.info(f"Dry run: {len(df)} rows parsed, {len(df.columns)} columns")
        return

    dates = df["submission_date"].unique()
    client = bigquery.Client(args.project)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="submission_date",
        ),
    )

    for date_val in dates:
        partition_df = df[df["submission_date"] == date_val]
        date_partition = str(date_val).replace("-", "")
        target_table = f"{args.project}.{args.dataset}.{args.table}${date_partition}"
        job = client.load_table_from_dataframe(
            partition_df, target_table, job_config=job_config
        )
        job.result()
        logging.info(f"Loaded {len(partition_df)} rows to {target_table}")


if __name__ == "__main__":
    main()
