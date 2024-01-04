"""Import Stripe reports into BigQuery."""

import os
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryFile
from time import sleep
from typing import Any, Dict, List, Optional

import requests
import rich_click as click
import stripe
import ujson
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from requests.auth import HTTPBasicAuth


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
        yield from (line + b"\n" for line in response.iter_lines())


@click.group("stripe", help="Commands for Stripe ETL.")
def stripe_():
    """Create the CLI group for stripe commands."""
    pass


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
    "--quiet",
    is_flag=True,
    help="Write output to os.devnull instead of sys.stdout",
)
@click.option(
    "--report-type",
    help="Stripe report type to import",
    required=True,
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
    quiet: bool,
    report_type: str,
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

    if table:
        handle = TemporaryFile(mode="w+b")
    elif quiet:
        handle = open(os.devnull, "w+b")
    else:
        handle = sys.stdout.buffer
    with handle as file_obj:
        path = Path(__file__).parent / f"{report_type}.schema.json"
        root = bigquery.SchemaField.from_api_repr(
            {"name": "root", "type": "RECORD", "fields": ujson.loads(path.read_text())}
        )
        columns = [f.name for f in root.fields]
        for row in _get_report_rows(
            api_key, after_date, before_date, report_type, columns
        ):
            file_obj.write(row)
        if table:
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
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                schema=root.fields,
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
                full_message = f"{job.job_id} failed: {e}"
                for error in job.errors or ():
                    message = error.get("message")
                    if message and message != getattr(e, "message", None):
                        full_message += "\n" + message
                raise click.ClickException(full_message)
            else:
                click.echo(f"{job.job_id} succeeded", file=sys.stderr)
