"""Import Stripe data into BigQuery."""

import os
import sys
import warnings
from datetime import datetime, timedelta, timezone
from tempfile import TemporaryFile
from typing import Any, Dict, Optional, Type

import click
import stripe
import ujson
from google.cloud import bigquery
from stripe.api_resources.abstract import ListableAPIResource

from .allowed_fields import ALLOWED_FIELDS, FilteredSchema


class StripeResourceType(click.ParamType):
    """Click parameter type for stripe listable resources."""

    name = "stripe resource"

    def convert(self, value, param, ctx):
        """Get a listable stripe resource type by name."""
        if isinstance(value, type) and issubclass(value, ListableAPIResource):
            return value

        if value.islower():
            value = value.capitalize()
        try:
            result = getattr(stripe, value)
        except AttributeError:
            self.fail(f"resource type {value!r} not found in stripe")
        if not issubclass(result, ListableAPIResource):
            self.fail("resource must be listable")
        return result


def _get_rows(
    api_key: Optional[str],
    date: Optional[datetime],
    before_date: Optional[datetime],
    resource: Type[ListableAPIResource],
):
    if api_key is None:
        yield from (ujson.loads(line) for line in sys.stdin)
    else:
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
        elif before_date:
            end = before_date.replace(tzinfo=timezone.utc)
            kwargs["created"] = {
                "lt": int(end.timestamp()),
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
            yield FilteredSchema.expand(row)


@click.group("stripe", help="Commands for Stripe ETL.")
def stripe_():
    """Create the CLI group for stripe commands."""
    pass


@stripe_.command("schema", help="Print BigQuery schema for Stripe resource types.")
@click.option(
    "--resource",
    default="Event",
    type=StripeResourceType(),
    help="Print the schema of this Stripe resource type",
)
def schema(resource: Type[ListableAPIResource]):
    """Print BigQuery schema for Stripe resource types."""
    filtered_schema = FilteredSchema(resource)
    click.echo(ujson.dumps([f.to_api_repr() for f in filtered_schema.filtered]))


@stripe_.command("import", help=__doc__)
@click.option(
    "--api-key",
    help="Stripe API key to use for authentication; If not set resources will be read "
    "from stdin",
)
@click.option(
    "--date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Creation date of resources to pull from stripe API; Added to --table "
    "to ensure only that date partition is replaced",
)
@click.option(
    "--before-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Only pull resources from stripe API with a creation date before this; "
    "Used when importing resources older than the earliest available events",
)
@click.option(
    "--table",
    help="BigQuery Standard SQL format table ID where resources will be written; "
    "if not set resources will be written to stdout",
)
@click.option(
    "--format-resources",
    is_flag=True,
    help="Format resources before writing to stdout, or after reading from stdin",
)
@click.option(
    "--strict-schema",
    is_flag=True,
    help="Throw an exception if an unexpected field is present in a resource",
)
@click.option(
    "--quiet",
    is_flag=True,
    help="Write output to os.devnull instead of sys.stdout",
)
@click.option(
    "--resource",
    default="Event",
    type=StripeResourceType(),
    help="Type of stripe resource to import",
)
def stripe_import(
    api_key: Optional[str],
    date: Optional[datetime],
    before_date: Optional[datetime],
    table: Optional[str],
    format_resources: bool,
    strict_schema: bool,
    quiet: bool,
    resource: Type[ListableAPIResource],
):
    """Import Stripe data into BigQuery."""
    if resource is stripe.Event and not date:
        click.echo("must specify --date for --resource=Event")
        sys.exit(1)
    if table and date:
        table = f"{table}${date:%Y%m%d}"
    if table:
        handle = TemporaryFile(mode="w+b")
    elif quiet:
        handle = open(os.devnull, "w+b")
    else:
        handle = sys.stdout.buffer
    with handle as file_obj:
        filtered_schema = FilteredSchema(resource)
        has_rows = False
        for row in _get_rows(api_key, date, before_date, resource):
            if format_resources or (table and api_key):
                row = filtered_schema.format_row(row, strict=strict_schema)
            file_obj.write(ujson.dumps(row).encode("UTF-8"))
            file_obj.write(b"\n")
            has_rows = True
        if not has_rows:
            click.echo(f"no {filtered_schema.type}s returned")
            sys.exit(1)
        elif table:
            if file_obj.writable():
                file_obj.seek(0)
            warnings.filterwarnings("ignore", module="google.auth._default")
            job_config = bigquery.LoadJobConfig(
                clustering_fields=["created"],
                ignore_unknown_values=False,
                schema=filtered_schema.filtered,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                time_partitioning=bigquery.TimePartitioning(field="created"),
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            if "$" in table:
                job_config.schema_update_options = [
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ]
            job = bigquery.Client().load_table_from_file(
                file_obj=file_obj,
                destination=table,
                job_config=job_config,
            )
            try:
                click.echo(f"Waiting for {job.job_id}", file=sys.stderr)
                job.result()
            except Exception as e:
                click.echo(f"{job.job_id} failed: {e}", file=sys.stderr)
                for error in job.errors or ():
                    message = error.get("message")
                    if message and message != getattr(e, "message", None):
                        click.echo(message, file=sys.stderr)
                sys.exit(1)
            else:
                click.echo(f"{job.job_id} succeeded", file=sys.stderr)
