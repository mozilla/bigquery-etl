"""Shared Statcounter CSV ingestion pipeline."""

import hashlib
import io
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import NamedTuple

import pandas as pd
import requests
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound

# No-op under Airflow (root logger already configured); applies locally only.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

GCS_BUCKET = "moz-fx-data-prod-external-data"
BQ_PROJECT = "moz-fx-data-shared-prod"
BQ_DATASET = "statcounter_derived"
DATE_COLUMN = "date"

SCHEMA = [
    bigquery.SchemaField("sk", "STRING"),
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("geography", "STRING"),
    bigquery.SchemaField("device", "STRING"),
    bigquery.SchemaField("browser", "STRING"),
    bigquery.SchemaField("percent", "FLOAT"),
]
PK_COLUMNS = ["date", "geography", "device", "browser"]
NUMERIC_COLUMNS = {"percent": (0, 100)}


class Source(NamedTuple):
    """A single Statcounter CSV source for a given geography and device."""

    geography: str
    device: str
    base_url: str  # date params added at runtime


@dataclass
class PipelineConfig:
    """Configuration for a Statcounter ingestion pipeline run."""

    sources: list[Source]
    gcs_blob_prefix: str
    bq_table: str
    clustering_fields: list[str]


def build_statcounter_url(base_url: str, date_from: date, date_to: date) -> str:
    """Append Statcounter date range parameters to a base URL.

    Args:
        base_url (str): Statcounter URL without date parameters.
        date_from (date): Start of the date range.
        date_to (date): End of the date range.

    Returns:
        str: Full CSV download URL with date parameters appended.
    """
    separator = "&" if "?" in base_url else "?"
    url = (
        f"{base_url}{separator}"
        f'fromInt={date_from.strftime("%Y%m%d")}'
        f'&toInt={date_to.strftime("%Y%m%d")}'
        f'&fromMonthYear={date_from.strftime("%Y-%m")}'
        f"&fromDay={date_from.day}"
        f'&toMonthYear={date_to.strftime("%Y-%m")}'
        f"&toDay={date_to.day}"
    )
    logger.info(f"Built URL for date range {date_from} to {date_to}")
    return url


def fetch_csv(url: str, geography: str, device: str) -> bytes:
    """Fetch CSV content from a Statcounter public download URL.

    Args:
        url (str): Statcounter CSV download URL (no auth required).
        geography (str): Geography label (e.g. 'worldwide').
        device (str): Device label (e.g. 'desktop').

    Returns:
        bytes: Raw CSV content.

    Raises:
        requests.HTTPError: If the response returns a non-2xx status code.
        ValueError: If the response body is empty or contains no data rows.
    """
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    content = response.content
    if not content.strip():
        raise ValueError("Response returned an empty body.")

    lines = content.decode("utf-8").strip().splitlines()
    if len(lines) < 2:
        raise ValueError("Response returned no data rows.")

    logger.info(
        f"Fetched {geography}/{device}: {len(content)} bytes ({len(lines) - 1} data rows)"
    )
    return content


def parse_csv(csv_content: bytes) -> pd.DataFrame:
    """Parse CSV content into a DataFrame.

    Args:
        csv_content (bytes): Raw CSV content.

    Returns:
        pd.DataFrame: Parsed CSV data.
    """
    df = pd.read_csv(io.StringIO(csv_content.decode("utf-8")))
    logger.info(f"Parsed {len(df)} rows, {len(df.columns)} columns")
    return df


def rename_columns(df: pd.DataFrame, partition_date: date) -> pd.DataFrame:
    """Rename Statcounter CSV columns to match the pipeline schema.

    The CSV has a fixed 'Browser' column and a dynamic percent column whose
    name includes the date (e.g. 'Market Share Perc. (DD MMMM YYYY)'). Parse
    the date from that header and compare it to partition_date so that a
    Statcounter fallback to a different day fails loudly instead of silently
    mis-stamping the row.

    Args:
        df (pd.DataFrame): Parsed CSV data.
        partition_date (date): The expected date of the response.

    Returns:
        pd.DataFrame: DataFrame with columns renamed to 'browser' and 'percent'.

    Raises:
        ValueError: If the percent column is missing, malformed, or its
            embedded date does not match partition_date.
    """
    percent_cols = [col for col in df.columns if col.startswith("Market Share")]
    if len(percent_cols) != 1:
        raise ValueError(
            f"Expected exactly one 'Market Share' column, found: {percent_cols}"
        )
    percent_col = percent_cols[0]

    match = re.search(r"\((\d{1,2} [A-Za-z]+ \d{4})\)", percent_col)
    if not match:
        raise ValueError(
            f"Could not parse date from percent column header: '{percent_col}'"
        )
    source_date = datetime.strptime(match.group(1), "%d %B %Y").date()
    if source_date != partition_date:
        raise ValueError(
            f"Source date in column header ({source_date}) does not match "
            f"partition_date ({partition_date}); Statcounter may have fallen "
            f"back to a different day. Column header: '{percent_col}'"
        )

    df = df.rename(columns={"Browser": "browser", percent_col: "percent"})
    logger.info(f"Renamed 'Browser' -> 'browser', '{percent_col}' -> 'percent'")
    return df


def add_metadata(
    df: pd.DataFrame, partition_date: date, geography: str, device: str
) -> pd.DataFrame:
    """Add date, geography, and device columns to the DataFrame.

    Args:
        df (pd.DataFrame): Parsed CSV data.
        partition_date (date): The date to assign to all rows.
        geography (str): Geography value to assign to all rows.
        device (str): Device value to assign to all rows.

    Returns:
        pd.DataFrame: DataFrame with date, geography, and device columns added.
    """
    df = df.assign(
        date=partition_date.isoformat(),
        geography=geography,
        device=device,
    )
    logger.info(
        f"Added metadata (date={partition_date}, geography={geography}, device={device}) to {len(df)} rows"
    )
    return df


def add_surrogate_key(df: pd.DataFrame) -> pd.DataFrame:
    """Add a surrogate key column (sk) as an MD5 hash of the natural key columns.

    Args:
        df (pd.DataFrame): Transformed CSV data.

    Returns:
        pd.DataFrame: DataFrame with sk column added.
    """
    # | is safe as a separator: date is always ISO format, device is Desktop/Mobile,
    # geography and browser come from Statcounter's fixed taxonomy — none contain |.
    df = df.assign(
        sk=df[PK_COLUMNS]
        .astype(str)
        .agg("|".join, axis=1)
        .apply(lambda s: hashlib.md5(s.encode()).hexdigest())
    )
    logger.info(f"Added surrogate key (sk) to {len(df)} rows")
    return df


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder DataFrame columns to match the schema field order.

    Args:
        df (pd.DataFrame): Transformed CSV data.

    Returns:
        pd.DataFrame: DataFrame with columns in schema order.
    """
    schema_order = ["sk", "date", "geography", "device", "browser", "percent"]
    df = df[schema_order]
    logger.info("Reordered columns to schema order")
    return df


def concatenate_dataframes(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate a list of DataFrames into one.

    Args:
        dfs (list[pd.DataFrame]): List of DataFrames to concatenate.

    Returns:
        pd.DataFrame: Concatenated DataFrame.
    """
    # ignore_index resets the index to avoid duplicate index values from each per-source DataFrame.
    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Concatenated {len(dfs)} DataFrames into {len(df)} total rows")
    return df


def validate_no_nulls(df: pd.DataFrame) -> None:
    """Check that PK columns contain no null or empty values.

    Args:
        df (pd.DataFrame): Transformed CSV data.

    Raises:
        ValueError: If a null or empty value is found in a PK column.
    """
    for col in PK_COLUMNS:
        if df[col].isnull().any():
            raise ValueError(f'Null value found in PK column "{col}".')
        # astype(str) converts nulls to "None", not "" — this check is for actual empty strings only.
        if (df[col].astype(str).str.strip() == "").any():
            raise ValueError(f'Empty value found in PK column "{col}".')
    logger.info("Null checks passed")


def validate_min_row_count(df: pd.DataFrame) -> None:
    """Check that the DataFrame contains at least one row.

    Args:
        df (pd.DataFrame): Transformed CSV data.

    Raises:
        ValueError: If the DataFrame contains no rows.
    """
    if len(df) == 0:
        raise ValueError("DataFrame contains no rows.")
    logger.info(f"Min row count check passed: {len(df)} rows")


def validate_unique_pk(df: pd.DataFrame) -> None:
    """Check that the DataFrame contains no duplicate rows on the PK columns.

    Args:
        df (pd.DataFrame): Transformed CSV data.

    Raises:
        ValueError: If duplicate PK values are detected.
    """
    if df.duplicated(subset=PK_COLUMNS).any():
        raise ValueError(f"Duplicate rows detected on PK columns: {PK_COLUMNS}.")
    logger.info("PK uniqueness check passed")


def validate_pk(df: pd.DataFrame) -> None:
    """Check that the DataFrame has no nulls and no duplicates on the PK columns.

    Args:
        df (pd.DataFrame): Transformed CSV data.
    """
    validate_no_nulls(df)
    validate_unique_pk(df)


def validate_numeric_bounds(df: pd.DataFrame) -> None:
    """Check that numeric columns fall within expected bounds.

    Args:
        df (pd.DataFrame): Transformed CSV data.

    Raises:
        ValueError: If a value is non-numeric or outside the expected bounds.
    """
    for col, (lower, upper) in NUMERIC_COLUMNS.items():
        numeric = pd.to_numeric(df[col], errors="coerce")
        non_numeric = df[col][numeric.isna() & df[col].notna()]
        if not non_numeric.empty:
            raise ValueError(
                f'Non-numeric values found in "{col}": {non_numeric.tolist()}'
            )
        if not numeric.between(lower, upper).all():
            raise ValueError(
                f'Values in "{col}" are outside expected bounds [{lower}, {upper}].'
            )
    logger.info("Numeric bounds checks passed")


def upload_to_gcs(df: pd.DataFrame, blob_name: str) -> None:
    """Upload transformed CSV content to GCS.

    Args:
        df (pd.DataFrame): Transformed CSV data.
        blob_name (str): GCS blob path.
    """
    gcs_uri = f"gs://{GCS_BUCKET}/{blob_name}"
    client = storage.Client()
    blob = client.bucket(GCS_BUCKET).blob(blob_name)
    blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")
    logger.info(f"Uploaded {len(df)} rows to {gcs_uri}")


def load_into_bigquery(
    partition_date: date, bq_table: str, blob_name: str, clustering_fields: list[str]
) -> None:
    """Load the CSV from GCS into a date partition of the BigQuery table.

    Uses WRITE_TRUNCATE to atomically replace the partition, making the operation idempotent.

    Args:
        partition_date (date): The partition date, used to construct the partition decorator.
        bq_table (str): BigQuery table name.
        blob_name (str): GCS blob path to load from.
        clustering_fields (list[str]): BigQuery clustering fields — must match the table definition.
    """
    gcs_uri = f"gs://{GCS_BUCKET}/{blob_name}"
    client = bigquery.Client(project=BQ_PROJECT)
    partition = partition_date.strftime("%Y%m%d")
    # $YYYYMMDD is BigQuery's partition decorator syntax — restricts the load to a single partition.
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{bq_table}${partition}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=DATE_COLUMN,
        ),
        clustering_fields=clustering_fields,
    )
    load_job = client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config,
    )
    load_job.result()
    logger.info(f"Loaded data into {table_ref}")


def count_bq_records(partition_date: date, bq_table: str) -> int:
    """Count records in the BigQuery table for a specific date.

    Args:
        partition_date (date): The date to filter on using DATE_COLUMN.
        bq_table (str): BigQuery table name.

    Returns:
        int: Number of records in the table for the given date.
    """
    client = bigquery.Client(project=BQ_PROJECT)
    table_ref = f"{BQ_PROJECT}.{BQ_DATASET}.{bq_table}"
    query = f"SELECT COUNT(*) AS total FROM `{table_ref}` WHERE {DATE_COLUMN} = @partition_date"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "partition_date", "DATE", partition_date.isoformat()
            )
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    count = rows[0].total
    logger.info(f"BigQuery record count for {partition_date}: {count}")
    return count


def compare_counts(csv_count: int, bq_count: int) -> None:
    """Compare CSV and BigQuery record counts and raise if they do not match.

    Args:
        csv_count (int): Number of records from the CSV.
        bq_count (int): Number of records loaded into BigQuery.

    Raises:
        ValueError: If the counts do not match.
    """
    if csv_count != bq_count:
        raise ValueError(
            f"Count mismatch: CSV has {csv_count}, BigQuery has {bq_count}"
        )
    logger.info(f"Record counts match: {csv_count}")


def ensure_dataset() -> None:
    """Create the BigQuery dataset if it does not already exist."""
    client = bigquery.Client(project=BQ_PROJECT)
    client.create_dataset(f"{BQ_PROJECT}.{BQ_DATASET}", exists_ok=True)
    logger.info(f"{BQ_PROJECT}.{BQ_DATASET} ready")


def ensure_table(config: PipelineConfig) -> None:
    """Create the BigQuery table with schema, partitioning, and clustering if it does not already exist.

    Args:
        config (PipelineConfig): Pipeline configuration.
    """
    client = bigquery.Client(project=BQ_PROJECT)
    table = bigquery.Table(
        f"{BQ_PROJECT}.{BQ_DATASET}.{config.bq_table}", schema=SCHEMA
    )
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field=DATE_COLUMN,
    )
    table.clustering_fields = config.clustering_fields
    client.create_table(table, exists_ok=True)
    logger.info(f"{table} ready")


def ensure_primary_key(bq_table: str) -> None:
    """Set primary key constraints on the BigQuery table if not already set.

    Args:
        bq_table (str): BigQuery table name.
    """
    client = bigquery.Client(project=BQ_PROJECT)
    table = client.get_table(f"{BQ_PROJECT}.{BQ_DATASET}.{bq_table}")
    existing = table.table_constraints
    if (
        not existing
        or not existing.primary_key
        or list(existing.primary_key.columns) != PK_COLUMNS
    ):
        table.table_constraints = bigquery.table.TableConstraints(
            primary_key=bigquery.table.PrimaryKey(columns=PK_COLUMNS),
            foreign_keys=[],
        )
        client.update_table(table, ["table_constraints"])
        logger.info(f"Set primary key on {bq_table}: {PK_COLUMNS}")
    else:
        logger.info(f"Primary key already set on {bq_table}, skipping")


def delete_from_gcs(blob_name: str) -> None:
    """Delete the CSV blob from GCS.

    Skip silently without raising if the blob does not exist.

    Args:
        blob_name (str): GCS blob path to delete.
    """
    gcs_uri = f"gs://{GCS_BUCKET}/{blob_name}"
    client = storage.Client()
    blob = client.bucket(GCS_BUCKET).blob(blob_name)
    try:
        blob.delete()
        logger.info(f"Deleted {gcs_uri}")
    except NotFound:
        logger.warning(f"{gcs_uri} already deleted, skipping")


def main(
    config: PipelineConfig,
    date_from: date | None = None,
    date_to: date | None = None,
) -> None:
    """Orchestrate the full CSV ingestion pipeline.

    Args:
        config (PipelineConfig): Pipeline configuration.
        date_from (date | None): Start of the date range. Defaults to yesterday.
        date_to (date | None): End of the date range. Defaults to date_from.
    """
    yesterday = date.today() - timedelta(days=1)
    # Default args can't use date.today() directly — it would be evaluated once at import time.
    date_from = date_from if date_from is not None else yesterday
    date_to = date_to if date_to is not None else date_from

    ensure_dataset()
    ensure_table(config)
    ensure_primary_key(config.bq_table)

    run_id = uuid.uuid4().hex
    dates = [
        date_from + timedelta(days=i) for i in range((date_to - date_from).days + 1)
    ]

    for partition_date in dates:
        dfs = []
        for source in config.sources:
            url = build_statcounter_url(source.base_url, partition_date, partition_date)
            csv_content = fetch_csv(url, source.geography, source.device)
            df = parse_csv(csv_content)
            df = rename_columns(df, partition_date)
            df = add_metadata(df, partition_date, source.geography, source.device)
            df = add_surrogate_key(df)
            df = reorder_columns(df)
            dfs.append(df)

        df = concatenate_dataframes(dfs)

        validate_min_row_count(df)
        validate_pk(df)
        validate_numeric_bounds(df)

        blob_name = (
            f'{config.gcs_blob_prefix}_{partition_date.strftime("%Y%m%d")}_{run_id}.csv'
        )
        upload_to_gcs(df, blob_name)
        try:
            load_into_bigquery(
                partition_date, config.bq_table, blob_name, config.clustering_fields
            )
        except Exception:
            delete_from_gcs(blob_name)
            raise
        bq_record_count = count_bq_records(partition_date, config.bq_table)
        compare_counts(len(df), bq_record_count)
        delete_from_gcs(blob_name)
