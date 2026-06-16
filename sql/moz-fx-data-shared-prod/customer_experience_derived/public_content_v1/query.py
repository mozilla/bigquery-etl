"""Ingest data from public content platforms into BigQuery."""

import html
import re
import time as time_module
import unicodedata
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

import pandas as pd
import requests
import rich_click as click
import yaml
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

THIS_PATH = Path(__file__)
PROJECT_ID = "moz-fx-data-shared-prod"
DATASET = THIS_PATH.parent.parent.name
TABLE_NAME = THIS_PATH.parent.name
REDDIT_PLACEHOLDERS = {"[deleted]", "[removed]"}
URL_PATTERN = re.compile(r"http\S+|www\.\S+")


class URL_SETTINGS(Enum):
    """Settings for the URL request to fetch data."""

    FX_REDDIT_URL = "https://www.reddit.com/r/firefox.json"
    PAGE_SIZE = 25
    MAX_POSTS = 1000
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:153.0) Gecko/20100101 Firefox/153.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    SCHEMA_PATH = THIS_PATH.parent / "schema.yaml"


def sanitize(value: str | None) -> str | None:
    """Clean text fields for NLP tasks including sentiment analysis, similarity, embeddings."""
    # - Convert Reddit placeholders and empty strings to None.
    if (
        not isinstance(value, str)
        or not value.strip()
        or value.strip() in REDDIT_PLACEHOLDERS
    ):
        return None

    # Decode HTML entities (e.g. &amp; -> &, &lt; -> <) for correct embeddings.
    value = html.unescape(value)

    # Normalize unicode characters to their canonical equivalents (smart quotes, em dashes, ligatures)
    value = unicodedata.normalize("NFKC", value)

    # Strip URLs as they add noise to Natural Language Processing models
    value = URL_PATTERN.sub("", value)

    # Collapse all whitespace variants (leading/trailing, newlines, tabs, multiple spaces)
    # into single spaces.
    value = " ".join(value.split())

    return value or None


def get_data_from_url() -> pd.DataFrame:
    """Paginate through the URL, collecting up to MAX_POSTS posts. The number of posts is limited by Reddit."""
    posts, after = [], None

    while len(posts) < URL_SETTINGS.MAX_POSTS.value:
        params = {
            "limit": URL_SETTINGS.PAGE_SIZE.value,
            **({"after": after} if after else {}),
        }
        # Non-200 responses (403, 429, 5xx) raise immediately via raise_for_status
        res = requests.get(
            URL_SETTINGS.FX_REDDIT_URL.value,
            headers=URL_SETTINGS.HEADERS.value,
            params=params,
        )
        res.raise_for_status()
        data = res.json()["data"]

        batch = [p["data"] for p in data["children"]]
        if not batch:
            if not posts:
                # assume no posts instead of a failure when the first page returned 200 with empty children.
                click.echo(
                    f"200 returned with no posts for {datetime.now(timezone.utc).date()} — assuming subreddit has no new posts."
                )
                break
            # If next page returns empty children after receiving posts, instead of an empty after cursor to signal the end.
            raise click.ClickException(
                f"Pagination failed on page {len(posts) // URL_SETTINGS.PAGE_SIZE.value + 1} after fetching {len(posts)} posts."
            )

        posts.extend(batch)
        after = data.get("after")
        if not after:
            break

        time_module.sleep(1)

    click.echo(f"Fetched {len(posts)} posts.")
    schema = yaml.safe_load(URL_SETTINGS.SCHEMA_PATH.value.read_text())
    columns = [
        f["name"] for f in schema["fields"] if f["name"] not in ("source", "metadata")
    ]
    # Fill missing columns with NaN instead of raising KeyError
    df = pd.DataFrame(posts).reindex(columns=columns)
    df["edited"] = df["edited"].apply(lambda x: x if isinstance(x, float) else None)
    df["creation_date"] = pd.to_datetime(df["created_utc"], unit="s").dt.date
    df["source"] = "reddit"
    df["metadata"] = [
        {"analysis_timestamp": datetime.now(timezone.utc)} for _ in range(len(df))
    ]
    return df


def write_to_bq(df: pd.DataFrame, project_id: str, dataset: str, table: str) -> None:
    """Ingest to a temp table and then MERGE into the final table using the post id."""
    client = bigquery.Client(project=project_id)
    destination_table_id = f"{project_id}.{dataset}.{table}"
    temp_table_id = f"{destination_table_id}_temp"

    # Check that table is already deployed to BigQuery
    try:
        prod_bq_table = client.get_table(destination_table_id)
    except NotFound:
        raise click.ClickException(
            f"Destination table {destination_table_id} does not exist in BigQuery. "
            f"Deploy the table and re-run."
        )

    # Sanitize text fields before writing to BQ
    for col in ("title", "selftext", "link_flair_text"):
        if col in df.columns:
            df[col] = df[col].apply(sanitize)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=prod_bq_table.schema,
    )

    click.echo(f"Loading {len(df)} rows to temp table.")
    client.load_table_from_dataframe(df, temp_table_id, job_config=job_config).result()

    temp_table = client.get_table(temp_table_id)
    if temp_table.num_rows > 0:
        click.echo(
            f"TEMP data ({temp_table.num_rows} rows) uploaded to " f"{temp_table_id}."
        )

    schema = yaml.safe_load(URL_SETTINGS.SCHEMA_PATH.value.read_text())
    update_fields = ", ".join(
        f"T.{col} = S.{col}"
        for col in [
            f["name"] for f in schema["fields"] if f["name"] not in ("id", "metadata")
        ]
    )

    # Merge today's data with existing data in production.
    merge_sql = f"""
            MERGE `{destination_table_id}` T
            USING `{temp_table_id}` S
            ON T.id = S.id
            WHEN MATCHED THEN
                UPDATE SET {update_fields}, T.metadata = S.metadata
            WHEN NOT MATCHED THEN
                INSERT ROW
        """
    click.echo(f"Merging into {destination_table_id}.")

    # Upload final data to production table.
    try:
        query_job = client.query(
            query=merge_sql,
            project=project_id,
        )
        query_job.result()
        # Delete temporary table after ingestion.
        client.delete_table(temp_table, not_found_ok=True)
        click.echo(
            f"Ingestion completed successfully. {len(df)} rows written to {destination_table_id}."
        )
    except Exception as e:
        click.echo(
            f"Ingestion to production table failed via MERGE with exception: {e}. The temporary table {temp_table_id} with new rows is available for debugging."
        )
        raise click.ClickException(str(e))


@click.command()
@click.option("--project_id", default=PROJECT_ID)
@click.option("--dataset", default=DATASET)
@click.option("--table", default=TABLE_NAME)
def main(project_id, dataset, table):
    """Fetch data and write to BigQuery."""
    payload = get_data_from_url()
    write_to_bq(payload, project_id, dataset, table)


if __name__ == "__main__":
    main()
