"""Import Stripe data into BigQuery."""

import os.path
import re
import sys
import warnings
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from tempfile import TemporaryFile
from typing import IO, Any, Dict, List, Optional, Type

import click
import stripe
import ujson
from google.cloud import bigquery
from stripe.api_resources.abstract import ListableAPIResource

# event data types with separate events and a defined schema
EVENT_DATA_TYPES = (
    stripe.Charge,
    stripe.CreditNote,
    stripe.Customer,
    stripe.Dispute,
    stripe.Invoice,
    stripe.PaymentIntent,
    stripe.Payout,
    stripe.Plan,
    stripe.Price,
    stripe.Product,
    stripe.SetupIntent,
    stripe.Subscription,
)


class StripeResourceType(click.ParamType):
    """Click parameter type for stripe listable resources."""

    name = "stripe resource"

    def convert(self, value, param, ctx):
        """Get a listable stripe resource type by name."""
        if not isinstance(value, str):
            self.fail(f"{value!r} is not a string")
        if value.islower():
            value = value.capitalize()
        try:
            result = getattr(stripe, value)
        except AttributeError:
            self.fail(f"resource type {value!r} not found in stripe")
        if not issubclass(result, ListableAPIResource):
            self.fail("resource must be listable")
        return result


def bigquery_format(obj: Any, *path):
    """Format stripe objects for BigQuery."""
    if isinstance(obj, stripe.ListObject):
        if obj.data and isinstance(obj.data[0], EVENT_DATA_TYPES):
            # don't expand lists of types that get updated in separate events
            return None
        # recursively request any additional values for paged lists.
        return [
            bigquery_format(e, *path, i) for i, e in enumerate(obj.auto_paging_iter())
        ]
    if path[-1:] == ("metadata",) and isinstance(obj, dict):
        if "userid" in obj:
            # hash fxa uid before it reaches BigQuery
            obj["fxa_uid"] = sha256(obj.pop("userid").encode()).hexdigest()
        # format metadata as a key-value list
        return [
            {"key": key, "value": bigquery_format(value, *path, key)}
            for key, value in obj.items()
        ]
    if isinstance(obj, dict):
        # recursively format and drop nulls, empty lists, and empty objects
        return {
            key: formatted
            for key, value in obj.items()
            # drop use_stripe_sdk because the contents are only for use in Stripe.js
            # https://stripe.com/docs/api/payment_intents/object#payment_intent_object-next_action-use_stripe_sdk
            if key != "use_stripe_sdk"
            for formatted in (bigquery_format(value, *path, key),)
            # drop nulls, empty lists, and empty objects
            if formatted not in (None, [], {})
        }
    return obj


def _open_file(
    api_key: Optional[str], file: Optional[str], table: Optional[str]
) -> IO[bytes]:
    if file is not None:
        if api_key is None:
            mode = "rb"
        else:
            mode = "w+b"
        return open(file, mode=mode)
    elif table is None:
        return sys.stdout.buffer
    elif api_key is None:
        return sys.stdin.buffer
    else:
        return TemporaryFile(mode="w+b")


def _get_schema(
    resource: Type[ListableAPIResource],
) -> Optional[List[bigquery.SchemaField]]:
    snake_case = re.sub(r"(?<!^)(?=[A-Z])", "_", resource.__name__).lower()
    path = os.path.join(os.path.dirname(__file__), f"{snake_case}.schema.json")
    if not os.path.exists(path):
        return None
    with open(path) as fp:
        return bigquery.SchemaField.from_api_repr(
            {"name": "root", "type": "RECORD", "fields": ujson.load(fp)}
        ).fields


@click.group(help="Commands for Stripe ETL.")
def stripe_():
    """Create the CLI group for stripe commands."""
    pass


@stripe_.command("import", help=__doc__)
@click.option(
    "--api-key",
    help="Stripe API key to use for authentication; If not set resources will be read "
    "from stdin, or --file if set",
)
@click.option(
    "--date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Creation date of resources to pull from stripe API; Added to table if set "
    "to ensure only that date partition is replaced",
)
@click.option(
    "--table",
    help="BigQuery Standard SQL format table ID where resources will be written; "
    "if not set resources will be writtent to stdout, or --file if set",
)
@click.option(
    "--file",
    help="Optional persistent file where resources are or will be stored in newline "
    "delimited json format for uploading to BigQuery",
)
@click.option(
    "--resource",
    default="Event",
    type=StripeResourceType(),
    help="Type of stripe resource to export",
)
def import_(
    api_key: str,
    date: datetime,
    table: str,
    file: str,
    resource: Type[ListableAPIResource],
):
    """Import Stripe data into BigQuery."""
    if resource is stripe.Event and not date:
        click.echo("must specify --date for --resource=Event")
        sys.exit(1)
    if api_key is None and table is None:
        click.echo("must specify --api-key and/or --table")
        sys.exit(1)
    if table and date:
        table = f"{table}${date:%Y%m%d}"
    with _open_file(api_key, file, table) as file_obj:
        if api_key:
            stripe.api_key = api_key
            kwargs: Dict[str, Any] = {}
            if date:
                start = date.replace(tzinfo=timezone.utc)
                kwargs["created"] = {
                    "gte": int(start.timestamp()),
                    # make sure to use timedelta before converting to timestamp,
                    # so that leap seconds are properly accounted for.
                    "lt": int((start + timedelta(days=1)).timestamp()),
                }
            if resource is stripe.Subscription:
                # list subscriptions api does not list canceled subscriptions by default
                # https://stripe.com/docs/api/subscriptions/list
                kwargs["status"] = "all"
            for instance in resource.list(**kwargs).auto_paging_iter():
                row: Dict[str, Any] = {
                    "created": datetime.utcfromtimestamp(instance.created).isoformat()
                }
                if resource is stripe.Event:
                    if not isinstance(instance.data.object, EVENT_DATA_TYPES):
                        continue  # skip events without a defined schema
                    row["data"] = {
                        instance.data.object.object.replace(
                            ".", "_"
                        ): instance.data.object,
                    }
                for key, value in instance.items():
                    if key not in row:
                        row[key] = value
                file_obj.write(ujson.dumps(bigquery_format(row)).encode("UTF-8"))
                file_obj.write(b"\n")
        if table:
            if file_obj.writable():
                file_obj.seek(0)
            warnings.filterwarnings("ignore", module="google.auth._default")
            job = bigquery.Client().load_table_from_file(
                file_obj=file_obj,
                destination=table,
                job_config=bigquery.LoadJobConfig(
                    clustering_fields=["created"],
                    ignore_unknown_values=False,
                    schema=_get_schema(resource),
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    time_partitioning=bigquery.TimePartitioning(field="created"),
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    schema_update_options=[
                        bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                    ],
                ),
            )
            try:
                print(f"Waiting for {job.job_id}", file=sys.stderr)
                job.result()
            except Exception as e:
                print(f"{job.job_id} failed: {e}", file=sys.stderr)
                for error in job.errors or ():
                    message = error.get("message")
                    if message and message != getattr(e, "message", None):
                        print(message, file=sys.stderr)
                sys.exit(1)
            else:
                print(f"{job.job_id} succeeded", file=sys.stderr)
