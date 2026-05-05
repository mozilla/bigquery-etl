# Import most recent major versions from: https://product-details.mozilla.org/1.0/firefox_versions.json

from argparse import ArgumentParser
from google.cloud import bigquery
import json
import requests
from pathlib import Path

URL = "https://product-details.mozilla.org/1.0/firefox_versions.json"
SCHEMA_FILE = Path(__file__).parent / "schema.yaml"

parser = ArgumentParser(description=__doc__)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--destination_dataset", default="telemetry_derived")
parser.add_argument("--destination_table", default="latest_versions_v2")


def main():
    args = parser.parse_args()
    client = bigquery.Client(args.project)
    destination_table = (
        f"{args.project}.{args.destination_dataset}.{args.destination_table}"
    )

    versions_json = json.loads(requests.get(URL).text)

    query = f"""
    SELECT 
      'release' AS channel,
      mozfun.norm.extract_version('{versions_json["LATEST_FIREFOX_VERSION"]}', 'major') AS latest_version
    UNION ALL
    SELECT
      'beta' AS channel,
      mozfun.norm.extract_version('{versions_json["FIREFOX_DEVEDITION"]}', 'major') AS latest_version
    UNION ALL
    SELECT
      'nightly' AS channel,
      mozfun.norm.extract_version('{versions_json["FIREFOX_NIGHTLY"]}', 'major') AS latest_version
    UNION ALL
    SELECT
      'esr' AS channel,
      mozfun.norm.extract_version('{versions_json["FIREFOX_ESR"]}', 'major') AS latest_version
    """

    job_config = bigquery.QueryJobConfig(
        destination=destination_table, write_disposition="WRITE_APPEND"
    )
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
    client.query(query, job_config=job_config).result()


if __name__ == "__main__":
    main()
