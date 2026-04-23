"""Generate vector embeddings for GitHub PR descriptions and load to BigQuery."""

import logging
import sys
from argparse import ArgumentParser

from google import genai
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.genai import types

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s: %(funcName)s: %(lineno)s: %(levelname)s: %(message)s",
)

EMBEDDING_MODEL = "text-embedding-004"
# BATCH_SIZE =  number of PRs (title + body) sent to Vertex AI in a single embedding API call
BATCH_SIZE = 5


def parse_args():
    """Parse command line arguments."""
    parser = ArgumentParser(
        description="Generate embeddings for GitHub PR descriptions and load to BigQuery."
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Date to process (YYYY-MM-DD). Embeds PRs with merged_at matching this date.",
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Fully qualified BigQuery source table or view (project.dataset.table).",
    )
    parser.add_argument(
        "--destination",
        required=True,
        help="Fully qualified BigQuery destination table (project.dataset.table).",
    )
    parser.add_argument(
        "--vertex-project",
        required=True,
        help="GCP project to use for Vertex AI embedding calls.",
    )
    parser.add_argument(
        "--vertex-location",
        default="us-west1",
        help="GCP region for Vertex AI (default: us-west1).",
    )
    return parser.parse_args()


def get_rows_for_date(source, date):
    """Query the source table and return PR rows for the given partition date."""
    client = bigquery.Client(project="moz-fx-data-shared-prod")
    query = f"""
        SELECT
            pr_number,
            repo_name,
            title,
            body,
            DATE(merged_at) AS merged_date
        FROM `{source}`
        WHERE DATE(merged_at) = '{date}'
    """
    rows = list(client.query(query).result())
    logging.info(f"Found {len(rows)} rows for {date} in {source}")
    return rows


def get_bq_row_count(destination, date):
    """Return the number of embedding rows already loaded for the given date.

    Args:
        destination: Fully qualified BQ table (project.dataset.table), e.g. moz-fx-data-shared-prod.github_external.pr_embeddings
        date: Partition date to check (YYYY-MM-DD), e.g. 2025-01-15
    """
    client = bigquery.Client(project="moz-fx-data-shared-prod")
    query = f"SELECT COUNT(*) AS cnt FROM `{destination}` WHERE merged_date = '{date}'"
    try:
        for row in client.query(query).result():
            return row.cnt
    except NotFound:
        logging.info(f"Destination table {destination} does not exist yet.")
    return 0


def generate_embeddings(texts, client):
    r"""Generate embeddings for a list of texts in batches.

    Args:
        texts: List of strings to embed, e.g. ["feat: add pipeline\n\nAdds a daily job...", "fix: handle rate limits"]
        client: Initialized google.genai.Client with vertexai=True

    Returns a list of embedding vectors in the same order as the input texts.
    Returns None for any text that is empty or fails to embed.
    """
    # Final list of embeddings — one entry per input text, in the same order
    results = []
    # Step through texts in chunks of BATCH_SIZE (e.g. 0-4, 5-9, 10-14...)
    for i in range(0, len(texts), BATCH_SIZE):
        # Slice out the current chunk of texts
        batch = texts[i : i + BATCH_SIZE]
        try:
            # Send the batch to Vertex AI — returns one embedding vector per text
            response = client.models.embed_content(
                model=EMBEDDING_MODEL,
                contents=batch,
            )
            # Extract the vector values from each embedding object
            results.extend(
                list(embedding_obj.values) for embedding_obj in response.embeddings
            )
            logging.info(f"Embedded batch {i // BATCH_SIZE + 1} ({len(batch)} texts)")
        except Exception as e:
            # Log and fill with None for each text in the failed batch
            logging.error(f"Embedding batch {i // BATCH_SIZE + 1} failed: {e}")
            results.extend([None] * len(batch))

    return results


def load_to_bq(records, destination):
    """Load embedding records to the partitioned BigQuery destination table."""
    logging.info(f"Starting load of embeddings to {destination}")
    client = bigquery.Client(project="moz-fx-data-shared-prod")
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("pr_number", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("repo_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("body", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("body_embedding", "FLOAT", mode="REPEATED"),
            bigquery.SchemaField("model_version", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("merged_date", "DATE", mode="REQUIRED"),
        ],
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="merged_date",
        ),
    )
    job = client.load_table_from_json(records, destination, job_config=job_config)
    job.result()
    logging.info(f"Loaded {len(records)} embedding records to {destination}")


def main():
    """Fetch PR rows for a given date, generate embeddings, and load to BigQuery."""
    args = parse_args()

    rows = get_rows_for_date(args.source, args.date)
    if not rows:
        logging.info(f"No rows found for {args.date}. Nothing to embed.")
        sys.exit(0)

    source_count = len(rows)
    existing_count = get_bq_row_count(args.destination, args.date)

    if existing_count == source_count:
        logging.info(
            f"Embeddings already loaded for {args.date} ({existing_count} rows). Skipping."
        )
        sys.exit(0)
    elif existing_count > 0:
        logging.error(
            f"Count mismatch for {args.date}: {existing_count} rows in {args.destination} "
            f"but {source_count} rows in {args.source}. "
            f"DELETE THE PARTITION in {args.destination} for {args.date} and re-run."
        )
        sys.exit(1)

    vertex_client = genai.Client(
        vertexai=True,
        project=args.vertex_project,
        location=args.vertex_location,
        http_options=types.HttpOptions(api_version="v1"),
    )

    texts = [f"{row.title}\n\n{row.body or ''}".strip() for row in rows]
    embeddings = generate_embeddings(texts, vertex_client)

    records = [
        {
            "pr_number": row.pr_number,
            "repo_name": row.repo_name,
            "title": row.title,
            "body": row.body,
            "body_embedding": embedding or [],
            "model_version": EMBEDDING_MODEL,
            "merged_date": str(row.merged_date),
        }
        for row, embedding in zip(rows, embeddings)
    ]

    load_to_bq(records, args.destination)
    logging.info("Done.")


if __name__ == "__main__":
    main()
