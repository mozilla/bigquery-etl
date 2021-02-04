import argparse
import datetime
import time

from google.cloud import bigquery_datatransfer
from google.cloud.bigquery_datatransfer import enums as transfer_enums


def cancel_active_transfers(project: str, transfer_config_name: str, transfer_location: str):
    client = bigquery_datatransfer.DataTransferServiceClient()
    play_store_transfer_config = client.location_transfer_config_path(
        project, transfer_location, transfer_config_name
    )

    transfer_run_iter = client.list_transfer_runs(
        play_store_transfer_config,
        states=[transfer_enums.TransferState.PENDING, transfer_enums.TransferState.RUNNING]
    )
    delete_count = 1
    for page in transfer_run_iter.pages:
        for transfer_run in page:
            if delete_count % 20 == 0:  # pause to avoid hitting api rate limit
                time.sleep(3)
            run_date = datetime.datetime.utcfromtimestamp(transfer_run.run_time.seconds).date()
            client.delete_transfer_run(transfer_run.name)
            delete_count += 1
            print(f"Cancelled {transfer_run.name} for date {run_date}")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project",
        type=str,
        required=True,
        help="Either the project that the source GCS project belongs to or "
             "the project that contains the transfer config"
    )
    parser.add_argument(
        "--transfer-config",
        type=str,
        required=True,
        help="ID of the transfer config. This should be a UUID."
    )
    parser.add_argument(
        "--transfer-location",
        type=str,
        default='us',
        help="Region of the transfer config (defaults to `us`)"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    cancel_active_transfers(args.project, args.transfer_config, args.transfer_location)
