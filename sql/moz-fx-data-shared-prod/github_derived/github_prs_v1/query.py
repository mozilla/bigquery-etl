"""Fetch merged GitHub pull requests from the GitHub API and load to BigQuery."""

import logging
import os
import sys
import time
from argparse import ArgumentParser

import requests
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s: %(funcName)s: %(lineno)s: %(levelname)s: %(message)s",
)

GITHUB_API_BASE_URL = "https://api.github.com"


def parse_args():
    """Parse command line arguments."""
    parser = ArgumentParser(description="Fetch merged GitHub PRs and load to BigQuery.")
    parser.add_argument(
        "--date",
        required=True,
        help="Date to fetch merged PRs for (YYYY-MM-DD format).",
    )
    parser.add_argument(
        "--destination",
        default="moz-fx-data-shared-prod.github_derived.github_prs_v1",
        help="BigQuery destination table.",
    )
    parser.add_argument(
        "--repo",
        action="append",
        dest="repos",
        default=None,
        help="GitHub repository in owner/repo format. Can be specified multiple times for additional repos.",
    )
    return parser.parse_args()


def make_github_request(url, headers, params=None, retries=5, backoff_factor=2):
    """Make a GitHub API request with exponential backoff and rate limit handling.

    Uses exponential backoff for transient errors (5xx). For GitHub rate limiting,
    sleeps until the reset time reported in X-RateLimit-Reset before retrying.
    """
    session = requests.Session()
    session.headers.update(headers)
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    for attempt in range(retries):
        try:
            response = session.get(url, params=params, timeout=30)
        except Exception as e:
            logging.error(f"Request to {url} failed: {e}")
            return None

        # Daily runs unlikely to hit 429 rate limit
        # For backfills, rate limit likely, sleeping until Github reported reset time
        is_rate_limited = response.status_code == 429 or (
            response.status_code == 403
            and response.headers.get("X-RateLimit-Remaining") == "0"
        )
        if is_rate_limited:
            reset_timestamp = int(response.headers.get("X-RateLimit-Reset", 0))
            sleep_seconds = max(reset_timestamp - time.time(), 0) + 5
            logging.warning(
                f"Rate limited by GitHub. Sleeping {sleep_seconds:.0f}s before retry "
                f"(attempt {attempt + 1}/{retries})"
            )
            time.sleep(sleep_seconds)
            continue

        return response

    logging.critical("Exceeded max retries due to GitHub rate limiting")
    sys.exit(1)


def get_pr_detail(pr_url, headers):
    """Call the individual PR endpoint and return a record matching schema.yaml."""
    response = make_github_request(pr_url, headers)
    if response is None:
        return None

    if not (200 <= response.status_code <= 299):
        logging.error(
            f"PR detail fetch returned {response.status_code}: {response.text}"
        )
        return None

    pr = response.json()

    return {
        "repo_name": pr["base"]["repo"]["full_name"],
        "pr_number": pr["number"],
        "title": pr["title"],
        "body": pr.get("body"),
        "author": pr["user"]["login"],
        "html_url": pr["html_url"],
        "created_at": pr["created_at"],
        "updated_at": pr["updated_at"],
        "merged_at": pr["merged_at"],
        "base_branch": pr["base"]["ref"],
        "head_branch": pr["head"]["ref"],
        "additions": pr.get("additions"),
        "deletions": pr.get("deletions"),
        "changed_files": pr.get("changed_files"),
        "commits": pr.get("commits"),
        "comments": pr.get("comments"),
        "review_comments": pr.get("review_comments"),
        "labels": [label["name"] for label in pr.get("labels", [])],
        "requested_reviewers": [r["login"] for r in pr.get("requested_reviewers", [])],
    }


def get_merged_prs(repo, date, token):
    """Call the GitHub search endpoint and fetch all PRs merged on the given date (YYYY-MM-DD)."""
    url = f"{GITHUB_API_BASE_URL}/search/issues"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    all_prs = []
    page = 1

    while True:
        params = {
            "q": f"repo:{repo} is:pr is:merged merged:{date}",
            "per_page": 100,
            "page": page,
        }

        response = make_github_request(url, headers, params=params)
        if response is None:
            logging.critical("Failed while fetching PRs from GitHub API")
            sys.exit(1)

        if not (200 <= response.status_code <= 299):
            logging.error(f"GitHub API returned {response.status_code}")
            logging.error(f"Response: {response.text}")
            logging.critical("Failed while fetching PRs from GitHub API")
            sys.exit(1)

        data = response.json()
        # Get total number of PRs to be pulled for selected date period
        # Useful for validating pagination handling worked correctly when PRs >= 100
        total_pr_count = data.get("total_count", 0)
        items = data.get("items", [])

        if page == 1:
            logging.info(
                f"GitHub reports {total_pr_count} merged PRs for {date} in {repo}"
            )

        for item in items:
            pr_url = item["pull_request"]["url"]
            pr = get_pr_detail(pr_url, headers)
            if pr:
                all_prs.append(pr)
        # Breaks once all PRs have been retrieved
        if len(all_prs) >= total_pr_count:
            break
        page += 1

    logging.info(
        f"Fetched {len(all_prs)} out of {total_pr_count} total merged PRs for {date} from {repo}"
    )
    return all_prs


def get_bq_row_count(destination, date, repo):
    """Return the number of rows already loaded for the given date and repo."""
    client = bigquery.Client(project="moz-fx-data-shared-prod")
    query = f"SELECT COUNT(*) as cnt FROM `{destination}` WHERE DATE(merged_at) = '{date}' AND repo_name = '{repo}'"
    result = client.query(query).result()
    for row in result:
        return row.cnt
    return 0


def get_github_pr_count(repo, date, token):
    """Return total merged PR count for the date from GitHub without fetching full detail."""
    url = f"{GITHUB_API_BASE_URL}/search/issues"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    params = {
        "q": f"repo:{repo} is:pr is:merged merged:{date}",
        "per_page": 1,
        "page": 1,
    }
    response = make_github_request(url, headers, params=params)
    if response is None:
        logging.error("Failed to get PR count from GitHub API")
        return None

    if not (200 <= response.status_code <= 299):
        logging.error(
            f"GitHub API returned {response.status_code} when checking PR count"
        )
        return None

    return response.json().get("total_count", 0)


def load_to_bq(records, destination):
    """Load PR records to a partitioned BigQuery table."""
    client = bigquery.Client(project="moz-fx-data-shared-prod")

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("repo_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("pr_number", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("title", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("body", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("author", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("html_url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("merged_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("base_branch", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("head_branch", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("additions", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("deletions", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("changed_files", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("commits", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("comments", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("review_comments", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("labels", "STRING", mode="REPEATED"),
            bigquery.SchemaField("requested_reviewers", "STRING", mode="REPEATED"),
        ],
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # Using JSON as daily PR pulls should be relatively small (few hundred rows)
        # Avoids having to write a csv and all that's required to manage that
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="merged_at",
        ),
        clustering_fields=["repo_name", "author"],
    )

    job = client.load_table_from_json(records, destination, job_config=job_config)
    job.result()
    logging.info(f"Loaded {len(records)} records to {destination}")


def main():
    """Fetch merged GitHub PRs for a given date and load to BigQuery."""
    args = parse_args()
    repos = args.repos or ["mozilla/bigquery-etl"]

    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        logging.critical("GITHUB_TOKEN environment variable not set")
        sys.exit(1)

    for repo in repos:
        logging.info(f"Processing {repo} for {args.date}")

        # Checks to see if data already present for date and repo
        existing_count = get_bq_row_count(args.destination, args.date, repo)
        if existing_count > 0:
            github_count = get_github_pr_count(repo, args.date, token)
            # If data present and complete -> skip this repo
            if existing_count == github_count:
                logging.info(
                    f"{repo}: destination table has {existing_count} PRs loaded for {args.date} which matches "
                    f"{github_count} PRs reported by GitHub. Load not required. Skipping."
                )
                continue
            else:
                # If data present but incomplete -> exits with failure
                logging.critical(
                    f"{repo}: {args.date} has {existing_count} PRs loaded but GitHub reports {github_count}. "
                    f"DATA IN TABLE IS WRONG! Manual investigation required. Delete incomplete data and rerun for the date"
                )
                sys.exit(1)

        records = get_merged_prs(repo, args.date, token)

        if records:
            load_to_bq(records, args.destination)
        else:
            logging.info(f"{repo}: no merged PRs found for {args.date}")

    logging.info("Done.")


if __name__ == "__main__":
    main()
