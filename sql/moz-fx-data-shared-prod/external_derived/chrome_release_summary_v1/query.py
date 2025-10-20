"""Summarize Chrome release updates from GCS files using OpenAI and write results back to GCS."""

# Load libraries
import os
import sys
from argparse import ArgumentParser
from datetime import datetime, timedelta

from google.cloud import storage
from openai import OpenAI

# Set variables
GCS_BUCKET = "gs://moz-fx-data-prod-external-data/"
BUCKET_NO_GS = "moz-fx-data-prod-external-data"

# Filepaths to read the data loaded to GCS by the "release_scraping DAG"
INPUT_FPATH_1 = "MARKET_RESEARCH/SCRAPED_INFO/ChromeReleaseNotes/WebScraping_"
INPUT_FPATH_2 = "MARKET_RESEARCH/SCRAPED_INFO/ChromeAI/WebScraping_"

# Filepaths to save ChatGPT Summaries to
OUTPUT_FPATH_1 = "MARKET_RESEARCH/SUMMARY_INFO/ChromeReleaseNotes/WebScraping_"
OUTPUT_FPATH_2 = "MARKET_RESEARCH/SUMMARY_INFO/ChromeAI/WebScraping_"

# Pull in the API key from GSM
OPENAI_API_TOKEN = os.getenv("DATA_ENG_OPEN_AI_API_KEY")

# If the API token is not found, raise an error
if not OPENAI_API_TOKEN:
    raise ValueError("Environment variable DATA_ENG_OPEN_AI_API_KEY is not set!")


def ensure_gcs_file_exists(gcs_path: str):
    """Check if a GCS file exists at the given path.

    Exit with an error if the file does not exist.

    Args:
        gcs_path (str): Full GCS path (e.g., 'gs://my-bucket/path/to/file.csv')
    """
    if not gcs_path.startswith("gs://"):
        print(f"Invalid GCS path: {gcs_path}")
        sys.exit(1)

    # Parse bucket and blob
    try:
        _, _, bucket_name, *path_parts = gcs_path.split("/", 3)
        blob_path = path_parts[0] if path_parts else ""
    except Exception:
        print(f"Unable to parse GCS path: {gcs_path}")
        sys.exit(1)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    if not blob.exists():
        print(f"File not found in GCS: {gcs_path}")
        sys.exit(1)
    else:
        print(f"File found: {gcs_path}")


def read_gcs_file(gcs_path: str) -> str:
    """Read the contents of a file in GCS and returns it as a string."""
    if not gcs_path.startswith("gs://"):
        raise ValueError(f"Invalid GCS path: {gcs_path}")

    # Parse bucket and blob
    _, _, bucket_name, *path_parts = gcs_path.split("/", 3)
    blob_path = path_parts[0] if path_parts else ""

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    if not blob.exists():
        raise FileNotFoundError(f"File not found: {gcs_path}")

    content = blob.download_as_text()  # or .download_as_bytes() for binary files
    return content


def main():
    """Pull scraped data from GCS, ask ChatGPT to summarize, save summary back to GCS."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()
    logical_dag_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    # The upstream DAG runs 3 days earlier
    upstream_dag_date = logical_dag_date - timedelta(days=3)
    logical_dag_date_str = logical_dag_date.strftime("%Y%m%d")
    upstream_dag_date_str = upstream_dag_date.strftime("%Y%m%d")

    # Check both input files exist, if not, error out
    gcs_fpath1 = GCS_BUCKET + INPUT_FPATH_1 + upstream_dag_date_str + ".txt"
    gcs_fpath2 = GCS_BUCKET + INPUT_FPATH_2 + upstream_dag_date_str + ".txt"

    ensure_gcs_file_exists(gcs_fpath1)
    ensure_gcs_file_exists(gcs_fpath2)

    # Make the output fpaths for storing the summaries received from ChatGPT
    final_output_fpath1 = OUTPUT_FPATH_1 + logical_dag_date_str + ".txt"
    final_output_fpath2 = OUTPUT_FPATH_2 + logical_dag_date_str + ".txt"

    # Read in the scraped data from the 2 files
    file_contents1 = read_gcs_file(gcs_fpath1)
    file_contents2 = read_gcs_file(gcs_fpath2)

    # Initialize the final output as an empty string
    final_output_1 = ""
    final_output_2 = ""

    # Open an Open AI Client
    client = OpenAI(api_key=OPENAI_API_TOKEN)

    # Ask ChatGPT to summarize scraped chrome release notes (fpath 1)
    prompt1 = "What new features has Chrome been working on recently?"
    resp1 = client.responses.create(
        model="gpt-4o-mini",
        input=[
            {"role": "system", "content": "You are an expert summarizer."},
            {"role": "user", "content": prompt1},
            {"role": "user", "content": file_contents1},
        ],
    )

    final_output_1 += (
        f"**Question:**\n{prompt1}\n\n"
        f"**Answer:**\n{resp1.output_text}\n\n"
        f"{'-'*80}\n\n"
    )

    # Ask ChatGPT to summarize Chrome AI updates (fpath 2)
    prompt2 = "What AI features has Chrome been working on recently?"
    resp2 = client.responses.create(
        model="gpt-4o-mini",
        input=[
            {"role": "system", "content": "You are an expert summarizer."},
            {"role": "user", "content": prompt2},
            {"role": "user", "content": file_contents2},
        ],
    )

    final_output_2 += (
        f"**Question:**\n{prompt2}\n\n"
        f"**Answer:**\n{resp2.output_text}\n\n"
        f"{'-'*80}\n\n"
    )

    # Save both summaries to GCS
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NO_GS)
    blob = bucket.blob(final_output_fpath1)
    blob.upload_from_string(final_output_1)
    print(f"Summary uploaded to gs://{BUCKET_NO_GS}/{final_output_fpath1}")

    blob2 = bucket.blob(final_output_fpath2)
    blob2.upload_from_string(final_output_2)
    print(f"Summary uploaded to gs://{BUCKET_NO_GS}/{final_output_fpath2}")


if __name__ == "__main__":
    main()
