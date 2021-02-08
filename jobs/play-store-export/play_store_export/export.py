import argparse
import datetime
import logging
import time
from typing import List

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery_datatransfer
from google.cloud.bigquery_datatransfer import enums as transfer_enums

# limit based on docs
# https://cloud.google.com/bigquery-transfer/docs/working-with-transfers#backfilling
BACKFILL_DAYS_MAX = 180


class DataTransferException(Exception):
    pass


def to_utc_midnight_datetime(base_date: datetime.date) -> datetime.datetime:
    return datetime.datetime(
        year=base_date.year, month=base_date.month, day=base_date.day,
        tzinfo=datetime.timezone.utc,
    )


def trigger_backfill(
        start_date: datetime.date, end_date: datetime.date,
        transfer_config_name: str,
        client: bigquery_datatransfer.DataTransferServiceClient
) -> List[bigquery_datatransfer.types.TransferRun]:
    """
    Start backfill of the given date range and return list of tuples of name and scheduled time
    for transfer runs created (1 per day)
    """
    logging.info(f"Starting backfill {start_date} to {end_date}")

    date_diff = (end_date - start_date).days
    if date_diff < 0:
        raise ValueError("End date must be equal to or greater than start date")
    if (end_date - start_date).days >= BACKFILL_DAYS_MAX:
        raise ValueError(f"A maximum of {BACKFILL_DAYS_MAX} days is allowed per backfill request")

    # end_date is midnight of the day after the last day
    end_date = to_utc_midnight_datetime(end_date) + datetime.timedelta(days=1)
    start_date = to_utc_midnight_datetime(start_date)

    response = client.start_manual_transfer_runs(
        parent=transfer_config_name,
        requested_time_range={
            "start_time": bigquery_datatransfer.types.Timestamp(
                seconds=int(start_date.timestamp())),
            "end_time": bigquery_datatransfer.types.Timestamp(
                seconds=int(end_date.timestamp())),
        },
    )
    return response.runs


def wait_for_transfer(transfer_name: str, timeout: int = 1200, polling_period: int = 20) -> int:
    """
    Continuously poll for run status to wait for completion
    """
    client = bigquery_datatransfer.DataTransferServiceClient()

    state = transfer_enums.TransferState.PENDING

    time_elapsed = 0
    while (state == transfer_enums.TransferState.PENDING or
           state == transfer_enums.TransferState.RUNNING):
        try:
            transfer_run = client.get_transfer_run(transfer_name)
        except GoogleAPICallError as e:
            # grpc errors are not serializable and cannot be raised in multiprocessing
            raise DataTransferException(f"Error getting transfer run: {e.message}")

        run_date = datetime.datetime.utcfromtimestamp(transfer_run.run_time.seconds).date()

        state = transfer_run.state

        if not (state == transfer_enums.TransferState.PENDING or
                state == transfer_enums.TransferState.RUNNING):
            break

        if time_elapsed >= timeout:
            logging.info(f"Transfer for {run_date} did not complete in {timeout} seconds")
            return -1
        time.sleep(polling_period)
        time_elapsed += polling_period

    if state == transfer_enums.TransferState.SUCCEEDED:
        result = "succeeded"
    elif state == transfer_enums.TransferState.CANCELLED:
        result = "cancelled"
    elif state == transfer_enums.TransferState.FAILED:
        result = "failed"
    else:
        result = "unspecified"

    logging.info(f"Transfer for {run_date} {result}")

    return state


def start_export(project: str, transfer_config_name: str, transfer_location: str,
                 base_date: datetime.date, backfill_day_count: int):
    """
    Start and wait for the completion of a backfill of `backfill_day_count` days, counting
    backwards from `base_date.  The base date is included in the backfill and counts as a
    day in the day count, i.e. `backfill_day_count` will backfill only .
    """
    if backfill_day_count <= 0:
        raise ValueError("Number of days to backfill must be at least 1")

    client = bigquery_datatransfer.DataTransferServiceClient()
    play_store_transfer_config = client.location_transfer_config_path(
        project, transfer_location, transfer_config_name
    )

    oldest_date = base_date - datetime.timedelta(days=backfill_day_count - 1)
    end_date = base_date

    logging.info(f"Backfilling {backfill_day_count} days: {oldest_date} to {base_date}")

    transfer_results = []
    # break backfills into BACKFILL_DAYS_MAX day segments
    while True:
        start_date = max(end_date - datetime.timedelta(days=BACKFILL_DAYS_MAX - 1), oldest_date)
        transfer_runs = trigger_backfill(start_date, end_date,
                                         play_store_transfer_config, client)
        transfer_run_names = [transfer_run.name for transfer_run
                              in sorted(transfer_runs, key=lambda run: run.schedule_time.seconds)]
        end_date = start_date - datetime.timedelta(days=1)

        # wait for backfill to complete
        # days in backfill are scheduled by the transfer service sequentially with 30s in between
        # starting from the latest date but can run in parallel
        new_results = map(wait_for_transfer, transfer_run_names)
        transfer_results.extend(new_results)

        if start_date == oldest_date:
            break
        elif start_date < oldest_date:
            raise ValueError("start_date should not be greater than oldest_date")

    successes = len([
        result for result in transfer_results
        if result == transfer_enums.TransferState.SUCCEEDED
    ])

    if len(transfer_results) != successes:
        raise DataTransferException(f"{len(transfer_results) - successes} failed dates")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        type=datetime.date.fromisoformat,
        required=True,
        help="Date at which the backfill will start, going backwards"
    )
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
    parser.add_argument(
        "--backfill-day-count",
        type=int,
        default=35,
        help="Number of days to backfill"
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = parse_args()
    start_export(args.project, args.transfer_config, args.transfer_location,
                 args.date, args.backfill_day_count)
