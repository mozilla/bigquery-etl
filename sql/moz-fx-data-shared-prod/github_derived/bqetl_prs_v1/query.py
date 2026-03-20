"""Fetch merged GitHub pull requests from the GitHub API and load to BigQuery."""

from bigquery_etl.github_derived import main

if __name__ == "__main__":
    main()
