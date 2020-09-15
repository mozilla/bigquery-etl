"""Import Stripe data into BigQuery."""

from argparse import ArgumentParser
from datetime import datetime, timedelta
from typing import Any, IO, Optional, Type
from tempfile import TemporaryFile
import ujson
import sys
import warnings

from google.cloud import bigquery
from stripe.api_resources.abstract import ListableAPIResource
import stripe


def date(value: str) -> datetime:
    """Parse a %Y-%m-%d format date.

    Return a datetime for access to the timestamp method.
    """
    # add Z and %z to parse as UTC
    return datetime.strptime(value + "Z", "%Y-%m-%d%z")


def resource(value: str) -> Type[ListableAPIResource]:
    """Get a listable stripe resource type by name."""
    if value.islower():
        result = getattr(stripe, value.capitalize())
    else:
        result = getattr(stripe, value)
    if not issubclass(result, ListableAPIResource):
        raise ValueError("resource not listable")
    return result


parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "--api-key",
    help="Stripe API key to use for authentication; If not set resources will be read "
    "from stdin, or --file if set",
)
parser.add_argument(
    "--date",
    type=date,
    help="%%Y-%%m-%%d format creation date of resource to pull from stripe API",
)
parser.add_argument(
    "--table",
    help="BigQuery Standard SQL format table ID where resources will be written; "
    "if not set resources will be writtent to stdout, or --file if set",
)
parser.add_argument(
    "--file",
    help="Optional persistent file where resources are or will be stored in newline "
    "delimited json format for uploading to BigQuery",
)
parser.add_argument(
    "--resource",
    default=stripe.Event,
    type=resource,
    help="Type of stripe resource to export",
)


def bigquery_format(obj: Any, *path):
    """Format stripe objects for BigQuery."""
    if isinstance(obj, stripe.ListObject):
        # recursively request any additional values for paged lists.
        return [
            bigquery_format(e, *path, i) for i, e in enumerate(obj.auto_paging_iter())
        ]
    if path[-1:] == ("metadata",) and isinstance(obj, dict):
        # format metadata as a key-value list
        return [
            {"key": key, "value": bigquery_format(value)} for key, value in obj.items()
        ]
    if isinstance(obj, dict):
        # recursively format and drop nulls, empty lists, and empty objects
        return {
            key: formatted
            for key, value in obj.items()
            for formatted in (bigquery_format(value, *path, key),)
            # drop nulls, empty lists, and empty objects
            if formatted not in (None, [], {})
        }
    return obj


def _open_file(
    api_key: Optional[str], file_: Optional[str], table: Optional[str]
) -> IO[bytes]:
    if file_ is not None:
        if api_key is None:
            mode = "rb"
        else:
            mode = "w+b"
        return open(file_, mode=mode)
    elif table is None:
        return sys.stdout.buffer
    elif api_key is None:
        return sys.stdin.buffer
    else:
        return TemporaryFile(mode="w+b")


def main():
    """Import Stripe data into BigQuery."""
    args = parser.parse_args()
    if args.resource == stripe.Event and not args.date:
        parser.error("must specify --date for --resource=Event")
    if args.api_key is None and args.table is None:
        parser.error("must specify --api-key and/or --table")
    if args.table and args.date:
        args.table = f"{args.table}${args.date:%Y%m%d}"
    with _open_file(args.api_key, args.file, args.table) as file_obj:
        if args.api_key:
            stripe.api_key = args.api_key
            created = {}
            if args.date:
                created = {
                    "gte": int(args.date.timestamp()),
                    # make sure to use timedelta before converting to timestamp,
                    # so that leap seconds are properly accounted for.
                    "lt": int((args.date + timedelta(days=1)).timestamp()),
                }
            for resource in args.resource.list(created=created).auto_paging_iter():
                row = {
                    "created": datetime.utcfromtimestamp(resource.created).isoformat()
                }
                if args.resource is stripe.Event:
                    row["data"] = {
                        resource.data.object.object.replace(
                            ".", "_"
                        ): resource.data.object,
                    }
                for key, value in resource.items():
                    if key not in row:
                        row[key] = value
                file_obj.write(ujson.dumps(bigquery_format(row)).encode("UTF-8"))
                file_obj.write(b"\n")
        if args.table:
            if file_obj.writable():
                file_obj.seek(0)
            job = bigquery.Client().load_table_from_file(
                file_obj=file_obj,
                destination=args.table,
                rewind=True,
                job_config=bigquery.LoadJobConfig(
                    clustering_fields=["created"],
                    ignore_unknown_values=False,
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    time_partitioning=bigquery.TimePartitioning(field="created"),
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                ),
            )
            try:
                print(f"Waiting for {job.job_id}", file=sys.stderr)
                job.result()
            except Exception as e:
                print(f"{job.job_id} failed: {e}", file=sys.stderr)
                sys.exit(1)
            else:
                print(f"{job.job_id} succeeded", file=sys.stderr)


if __name__ == "__main__":
    warnings.filterwarnings("ignore", module="google.auth._default")
    main()
