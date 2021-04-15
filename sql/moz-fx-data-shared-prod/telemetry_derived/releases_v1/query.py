# Import release data from https://product-details.mozilla.org/1.0/firefox.json

from argparse import ArgumentParser
from google.cloud import bigquery
import io
import json
import requests
from pathlib import Path
import yaml

URL = "https://product-details.mozilla.org/1.0/firefox.json"
SCHEMA_FILE = Path(__file__).parent / "schema.yaml"

parser = ArgumentParser(description=__doc__)
parser.add_argument("--project", default="moz-fx-data-shared-prod")
parser.add_argument("--destination_dataset", default="telemetry_derived")
parser.add_argument("--destination_table", default="releases_v1")


def main():
    args = parser.parse_args()
    client = bigquery.Client(args.project)

    release_json = json.loads(requests.get(URL).text)
    # ignoring the key here since it is <product>-<version>
    release_data = [r for _, r in release_json["releases"].items()]

    schema_dict = yaml.safe_load(SCHEMA_FILE.read_text())["fields"]
    schema = client.schema_from_json(io.StringIO(json.dumps(schema_dict)))

    job_config = bigquery.LoadJobConfig(schema=schema)
    job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE

    job = client.load_table_from_json(
        release_data,
        f"{args.destination_dataset}.{args.destination_table}",
        job_config=job_config,
    )
    job.result()


if __name__ == "__main__":
    main()
