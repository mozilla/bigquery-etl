"""Import Stripe data into BigQuery."""

import sys
import warnings
from datetime import datetime, timedelta, timezone
from tempfile import TemporaryFile
from typing import IO, Any, Dict, Optional, Type

import click
import stripe
from google.cloud import bigquery
from stripe.api_resources.abstract import ListableAPIResource

from .allowed_fields import ALLOWED_FIELDS, FilteredSchema


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
        filtered_schema = FilteredSchema(resource)
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
                    event_resource = instance.data.object.object.replace(".", "_")
                    if event_resource not in ALLOWED_FIELDS["event"]["data"]:
                        continue  # skip events for resources that aren't allowed
                    row["data"] = {
                        event_resource: instance.data.object,
                    }
                for key, value in instance.items():
                    if key not in row:
                        row[key] = value
                file_obj.write(filtered_schema.format_row(row))
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
                    schema=filtered_schema.filtered,
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
