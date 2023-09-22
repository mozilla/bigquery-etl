"""Import Apple reports into BigQuery."""

import gzip
import sys
import time
import warnings
from datetime import datetime
from io import BytesIO
from tempfile import TemporaryFile
from typing import Optional

import click
import requests
from authlib.jose import jwt
from google.cloud import bigquery

SUBSCRIBER_SCHEMA = [
    bigquery.SchemaField("report_date", "DATE"),
    bigquery.SchemaField("event_date", "DATE"),
    bigquery.SchemaField("app_name", "STRING"),
    bigquery.SchemaField("app_apple_id", "INT64"),
    bigquery.SchemaField("subscription_name", "STRING"),
    bigquery.SchemaField("subscription_apple_id", "INT64"),
    bigquery.SchemaField("subscription_group_id", "INT64"),
    bigquery.SchemaField("standard_subscription_duration", "STRING"),
    bigquery.SchemaField("subscription_offer_name", "STRING"),
    bigquery.SchemaField("promotional_offer_id", "STRING"),
    bigquery.SchemaField("subscription_offer_type", "STRING"),
    bigquery.SchemaField("subscription_offer_duration", "STRING"),
    bigquery.SchemaField("marketing_opt_in_duration", "STRING"),
    bigquery.SchemaField("customer_price", "NUMERIC"),
    bigquery.SchemaField("customer_currency", "STRING"),
    bigquery.SchemaField("developer_proceeds", "NUMERIC"),
    bigquery.SchemaField("proceeds_currency", "STRING"),
    bigquery.SchemaField("preserved_pricing", "STRING"),
    bigquery.SchemaField("proceeds_reason", "STRING"),
    bigquery.SchemaField("client", "STRING"),
    bigquery.SchemaField("device", "STRING"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("subscriber_id", "INT64"),
    bigquery.SchemaField("subscriber_id_reset", "STRING"),
    bigquery.SchemaField("refund", "STRING"),
    bigquery.SchemaField("purchase_date", "DATE"),
    bigquery.SchemaField("units", "NUMERIC"),
]


def _get_lines(
    key_id: Optional[str],
    issuer_id: Optional[str],
    private_key: Optional[str],
    vendor_number: Optional[str],
    report_date: Optional[datetime],
):
    if not all((key_id, issuer_id, private_key, vendor_number)):
        yield from sys.stdin.buffer
    else:
        expiration_time = int(round(time.time() + (20 * 60)))  # 20 minutes
        token = jwt.encode(
            {"alg": "ES256", "kid": key_id, "typ": "JWT"},
            {"iss": issuer_id, "exp": expiration_time, "aud": "appstoreconnect-v1"},
            private_key,
        )
        try:
            response = requests.get(
                "https://api.appstoreconnect.apple.com/v1/salesReports",
                params={
                    "filter[frequency]": "DAILY",
                    "filter[reportDate]": f"{report_date:%F}",
                    "filter[reportSubType]": "DETAILED",
                    "filter[reportType]": "SUBSCRIBER",
                    "filter[vendorNumber]": vendor_number,
                    "filter[version]": "1_3",
                },
                headers={"Authorization": "Bearer " + token.decode()},
            )
        except requests.RequestException as e:
            raise click.ClickException(str(e))
        try:
            response.raise_for_status()
        except requests.RequestException as e:
            raise click.ClickException(f"{e}\n" + response.content.decode("UTF-8"))
        yield from BytesIO(gzip.decompress(response.content))


@click.group(help="Commands for Apple Reports ETL.")
def apple():
    """CLI group for apple commands."""
    pass


@apple.command("import")
@click.option(
    "--key-id",
    help="API JWT key id; Must be specified with --issuer-id, --private-key, and "
    "--vendor-number; If not set rows will be read from stdin",
)
@click.option(
    "--issuer-id",
    help="API JWT issuer id; Must be specified with --key-id, --private-key, and "
    "--vendor-number; If not set rows will be read from stdin",
)
@click.option(
    "--private-key",
    help="API JWT private key; Must be specified with --key-id, --issuer-id, and "
    "--vendor-number; If not set rows will be read from stdin",
)
@click.option(
    "--vendor-number",
    help="Apple vendor number; Must be specified with --key-id, --issuer-id, and "
    "--private-key; If not set rows will be read from stdin",
)
@click.option(
    "--date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Report date to pull from API; Added to --table to ensure only that date "
    "partition is replaced",
)
@click.option(
    "--table",
    help="BigQuery Standard SQL format table ID where resources will be written; if "
    "not set resources will be written to stdout",
)
def apple_import(
    key_id: Optional[str],
    issuer_id: Optional[str],
    private_key: Optional[str],
    vendor_number: Optional[str],
    date: Optional[datetime],
    table: Optional[str],
):
    """Import Apple Subscriber report into BigQuery."""
    auth_options = (key_id, issuer_id, private_key, vendor_number)
    if any(auth_options) and not all(auth_options):
        raise click.ClickException(
            "Must set all of --key-id, --issuer-id, --private-key, and "
            "--vendor-number or none of them"
        )
    if (table or key_id) and not date:
        raise click.ClickException("must specify --date")
    if table and date:
        table = f"{table}${date:%Y%m%d}"
    with TemporaryFile(mode="w+b") if table else sys.stdout.buffer as file_obj:
        has_rows = False
        report_date = f"{date:%F}\t".encode("UTF-8")
        for line in _get_lines(key_id, issuer_id, private_key, vendor_number, date):
            file_obj.write(report_date)  # prefix lines with report date column
            file_obj.write(line)
            has_rows = True
        if not has_rows:
            raise click.ClickException("no rows returned")
        elif table:
            if file_obj.writable():
                file_obj.seek(0)
            warnings.filterwarnings("ignore", module="google.auth._default")
            with bigquery.Client() as bq:
                job = bq.load_table_from_file(
                    file_obj=file_obj,
                    destination=table,
                    job_config=bigquery.LoadJobConfig(
                        allow_jagged_rows=False,
                        field_delimiter="\t",
                        ignore_unknown_values=False,
                        schema=SUBSCRIBER_SCHEMA,  # only report type supported
                        schema_update_options=[
                            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                        ],
                        skip_leading_rows=1,
                        source_format=bigquery.SourceFormat.CSV,
                        time_partitioning=bigquery.TimePartitioning(
                            field="report_date"
                        ),
                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    ),
                )
                try:
                    click.echo(f"Waiting for {job.job_id}", file=sys.stderr)
                    job.result()
                except Exception as e:
                    message = f"{job.job_id} failed: {e}"
                    for error in job.errors or ():
                        error_message = error.get("message")
                        if error_message and error_message != getattr(
                            e, "message", None
                        ):
                            message += f"\n{error_message}"
                    raise click.ClickException(message)
                else:
                    click.echo(f"{job.job_id} succeeded", file=sys.stderr)
