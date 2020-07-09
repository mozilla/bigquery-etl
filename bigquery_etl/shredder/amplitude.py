"""Forward deletion requests from BigQuery to Amplitude."""

from argparse import ArgumentParser
from datetime import datetime
from multiprocessing.pool import ThreadPool
from time import time, sleep
from os import environ
import warnings
import logging
import json

from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests

from ..util import standard_args


AMPLITUDE_API_KEY = "AMPLITUDE_API_KEY"
AMPLITUDE_SECRET_KEY = "AMPLITUDE_SECRET_KEY"

parser = ArgumentParser(description=__doc__)
standard_args.add_argument(
    parser,
    "--date",
    type=lambda d: datetime.strptime(d, "%Y-%m-%d").date(),
    required=True,
    help="Deletion request date to forward to amplitude",
)
standard_args.add_argument(
    parser,
    "--table-id",
    "--table_id",
    required=True,
    help="Deletion request table to forward to amplitude",
)

group = parser.add_mutually_exclusive_group(required=True)
standard_args.add_argument(
    group,
    "--user-id-field",
    "--user_id_field",
    help="Field in --table-id that maps to user_id in amplitude",
)
standard_args.add_argument(
    group,
    "--device-id-field",
    "--device_id_field",
    help="Field in --table-id that maps to device_id in amplitude;"
    " Do not use unless --user-id-field isn't available",
)

standard_args.add_argument(
    parser,
    "--api-key",
    "--api_key",
    default=environ.get(AMPLITUDE_API_KEY),
    required=AMPLITUDE_API_KEY not in environ,
    help="Amplitude API key; used for identify and deletions api calls",
)
standard_args.add_argument(
    parser,
    "--secret-key",
    "--secret_key",
    default=environ.get(AMPLITUDE_SECRET_KEY),
    required=AMPLITUDE_SECRET_KEY not in environ,
    help="Amplitude secret key; used for deletions api calls",
)
standard_args.add_log_level(parser)


def main():
    """Read deletion requests from bigquery and send them to amplitude."""
    args = parser.parse_args()

    requests_session = requests.Session()
    requests_session.mount(
        "https://",
        HTTPAdapter(
            max_retries=Retry(
                total=3,
                status_forcelist=[429, 500, 502, 503, 504],
                method_whitelist=["POST"],
            )
        ),
    )

    ids = [
        row[0]
        for row in bigquery.Client().query(
            f"SELECT {args.user_id_field or args.device_id_field} FROM {args.table_id}"
            f" WHERE DATE(submission_timestamp) = DATE '{args.date}'"
        )
    ]

    # associate device_ids to matching user_ids so that they can be deleted
    if args.user_id_field is None:
        identify_parallelism, identify_batch_size = 100, 10

        def _identify(start):
            # this api is limited to 100 batches per second, so don't return any faster
            # https://help.amplitude.com/hc/en-us/articles/360032842391-HTTP-API-V2#upload-limit
            earliest_return = time() + identify_parallelism / 100
            end = start + identify_batch_size
            requests_session.post(
                "https://api2.amplitude.com/identify",
                data={
                    "api_key": args.api_key,
                    "identification": json.dumps(
                        [{"device_id": id, "user_id": id} for id in ids[start:end]]
                    ),
                },
            ).raise_for_status()
            # sleep to match api speed limit if necessary
            sleep(max(0, earliest_return - time()))

        with ThreadPool(identify_parallelism) as pool:
            logging.info("Identifying relevant device_ids as user_ids")
            pool.map(_identify, range(0, len(ids), identify_batch_size), chunksize=1)

    deletions_parallelism, deletions_batch_size = 1, 100

    def _deletions(start):
        # this api is limited to 1 batch per second, so don't return any faster
        # https://help.amplitude.com/hc/en-us/articles/360000398191-User-Privacy-API#h_2beded8a-5c39-4113-a847-551b7151339b
        earliest_return = time() + deletions_parallelism
        end = start + deletions_batch_size
        requests_session.post(
            "https://amplitude.com/api/2/deletions/users",
            auth=(args.api_key, args.secret_key),
            json={
                "user_ids": ids[start:end],
                "requester": "shredder",
                "ignore_invalid_id": "True",
                "delete_from_org": "False",
            },
        ).raise_for_status()
        # sleep to match api speed limit if necessary
        sleep(max(0, earliest_return - time()))

    with ThreadPool(deletions_parallelism) as pool:
        logging.info("Submitting deletions")
        pool.map(_deletions, range(0, len(ids), deletions_batch_size), chunksize=1)


if __name__ == "__main__":
    warnings.filterwarnings("ignore", module="google.auth._default")
    main()
