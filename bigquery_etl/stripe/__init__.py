"""Import Stripe data into BigQuery."""

import os
import sys
import warnings
from datetime import datetime, timezone
from tempfile import TemporaryFile
from time import sleep
from typing import Any, Dict, List, Optional, Type

import click
import requests
import stripe
import ujson
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from requests.auth import HTTPBasicAuth
from stripe.api_resources.abstract import ListableAPIResource

from .allowed_fields import ALLOWED_FIELDS, FilteredSchema, get_rooted_schema


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


def _get_report_rows(
    api_key: Optional[str],
    after_date: Optional[datetime],
    before_date: Optional[datetime],
    report_type: str,
    columns: List[str],
):
    if api_key is None:
        yield from sys.stdin.buffer
    else:
        stripe.api_key = api_key
        parameters: Dict[str, Any] = {"columns": columns}
        if after_date:
            parameters["interval_start"] = int(after_date.timestamp())
        if before_date:
            parameters["interval_end"] = int(before_date.timestamp())

        try:
            run = stripe.reporting.ReportRun.create(
                report_type=report_type,
                parameters=parameters,
            )
        except stripe.error.InvalidRequestError as e:
            # Wrap exception to hide unnecessary traceback
            raise click.ClickException(str(e))

        click.echo(f"Waiting on report {run.id!r}", file=sys.stderr)
        # wait up to 30 minutes for report to finish
        timeout = datetime.utcnow() + relativedelta(minutes=30)
        while datetime.utcnow() < timeout:
            if run.status != "pending":
                break
            sleep(10)
            run.refresh()
        if run.status != "succeeded":
            raise click.ClickException(
                f"Report {run.id!r} did not succeed, status was {run.status!r}"
            )
        response = requests.get(
            run.result.url, auth=HTTPBasicAuth(api_key, ""), stream=True
        )
        response.raise_for_status()
        yield from response.iter_lines()


def _get_rows(
    api_key: Optional[str],
    after_date: Optional[datetime],
    before_date: Optional[datetime],
    resource: Type[ListableAPIResource],
):
    if api_key is None:
        yield from (ujson.loads(line) for line in sys.stdin)
    else:
        stripe.api_key = api_key
        kwargs: Dict[str, Any] = {}
        if after_date or before_date:
            kwargs["created"] = {}
            if after_date:
                kwargs["created"]["gte"] = int(after_date.timestamp())
            if before_date:
                kwargs["created"]["lt"] = int(before_date.timestamp())
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
    type=click.DateTime(formats=["%Y-%m-%d", "%Y-%m", "%Y"]),
    help="Creation date of resources to pull from stripe API; Added to --table "
    "to ensure only that date partition is replaced",
)
@click.option(
    "--after-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Only pull resources from stripe API with a creation date on or after this; "
    "Used when importing resources older than the earliest available events",
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
    type=StripeResourceType(),
    help="Type of stripe resource to import",
)
@click.option(
    "--report-type",
    help="Stripe report type to import",
)
@click.option(
    "--time-partitioning-field",
    default="created",
    help="Field to use for partitioning and clustering; if --date or --before-date or"
    " --after-date are specified, values must fall within that window",
)
@click.option(
    "--time-partitioning-type",
    default=bigquery.TimePartitioningType.DAY,
    type=click.Choice(
        [
            bigquery.TimePartitioningType.DAY,
            bigquery.TimePartitioningType.MONTH,
            bigquery.TimePartitioningType.YEAR,
        ]
    ),
    help="BigQuery time partitioning type for --table",
)
def stripe_import(
    api_key: Optional[str],
    date: Optional[datetime],
    after_date: Optional[datetime],
    before_date: Optional[datetime],
    table: Optional[str],
    format_resources: bool,
    strict_schema: bool,
    quiet: bool,
    resource: Optional[Type[ListableAPIResource]],
    report_type: Optional[str],
    time_partitioning_field: str,
    time_partitioning_type: str,
):
    """Import Stripe data into BigQuery."""
    if after_date:
        after_date = after_date.replace(tzinfo=timezone.utc)
    if before_date:
        before_date = before_date.replace(tzinfo=timezone.utc)
    if date:
        date = date.replace(tzinfo=timezone.utc)
        if time_partitioning_type == bigquery.TimePartitioningType.DAY:
            after_date = date
            before_date = after_date + relativedelta(days=1)
            if table:
                table = f"{table}${date:%Y%m%d}"
        elif time_partitioning_type == bigquery.TimePartitioningType.MONTH:
            after_date = date.replace(day=1)
            before_date = after_date + relativedelta(months=1)
            if table:
                table = f"{table}${date:%Y%m}"
        elif time_partitioning_type == bigquery.TimePartitioningType.YEAR:
            after_date = date.replace(month=1, day=1)
            before_date = after_date + relativedelta(years=1)
            if table:
                table = f"{table}${date:%Y}"
    if resource is stripe.Event and not date:
        raise click.ClickException("must specify --date for --resource=Event")
    if not report_type and not resource:
        raise click.ClickException("must specify --resource or --report-type")
    if report_type and resource:
        raise click.ClickException("cannot specify both --resource and --report-type")

    if table:
        handle = TemporaryFile(mode="w+b")
    elif quiet:
        handle = open(os.devnull, "w+b")
    else:
        handle = sys.stdout.buffer
    with handle as file_obj:
        has_rows = False
        if resource:
            filtered_schema = FilteredSchema(resource)
            for row in _get_rows(api_key, after_date, before_date, resource):
                if format_resources or (table and api_key):
                    row = filtered_schema.format_row(row, strict=strict_schema)
                file_obj.write(ujson.dumps(row).encode("UTF-8"))
                file_obj.write(b"\n")
                has_rows = True
        elif report_type:
            root = get_rooted_schema(report_type)
            columns = [f.name for f in root.fields]
            for row in _get_report_rows(
                api_key, after_date, before_date, report_type, columns
            ):
                file_obj.write(row)
                file_obj.write(b"\n")
                # stripe api enforces that data is available for the requested range, so
                # allow headers row to count for has_rows, because it means the report
                # is complete, and valid reports may be empty, e.g. when pulled daily
                has_rows = True
        if not has_rows:
            raise click.ClickException("no rows returned")
        elif table:
            if file_obj.writable():
                file_obj.seek(0)
            warnings.filterwarnings("ignore", module="google.auth._default")
            job_config = bigquery.LoadJobConfig(
                clustering_fields=[time_partitioning_field],
                ignore_unknown_values=False,
                time_partitioning=bigquery.TimePartitioning(
                    field=time_partitioning_field, type_=time_partitioning_type
                ),
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            if resource:
                job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
                job_config.schema = filtered_schema.filtered
            elif report_type:
                job_config.source_format = bigquery.SourceFormat.CSV
                job_config.skip_leading_rows = 1
                job_config.schema = root.fields
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
                full_message = f"{job.job_id} failed: {e}"
                for error in job.errors or ():
                    message = error.get("message")
                    if message and message != getattr(e, "message", None):
                        full_message += "\n" + message
                raise click.ClickException(full_message)
            else:
                click.echo(f"{job.job_id} succeeded", file=sys.stderr)
