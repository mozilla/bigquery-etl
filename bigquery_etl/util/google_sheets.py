"""Helper functions for Google Sheets."""

from copy import deepcopy
from dataclasses import dataclass
from textwrap import dedent
from typing import Collection, Optional, cast

import google.auth
from google.cloud import bigquery

from bigquery_etl.schema import Schema


@dataclass
class GoogleSheetColumnTransform:
    """Google Sheet column transformation specification.

    :param column: Column name.
    :param sql: SQL to transform the column.
    :param sheet_column_type: Optional original type of the Google Sheet column.
        This needs to be specified if the transformation is changing the column's type.
    """

    column: str
    sql: str
    sheet_column_type: Optional[str] = None


def import_google_sheet(
    destination_table: str,
    destination_schema: Schema,
    sheet_url: str,
    sheet_range: Optional[str] = None,
    skip_leading_rows: Optional[int] = None,
    where_sql: Optional[str] = None,
    column_transforms: Optional[Collection[GoogleSheetColumnTransform]] = None,
) -> None:
    """Import data from a Google Sheet into a BigQuery table, overwriting the table.

    :param destination_table: Fully qualified ID of the destination table.
    :param destination_schema: Schema of the destination table.
    :param sheet_url: URL of the Google Sheet to import data from.
    :param sheet_range: Optional range in the Google Sheet to import data from.
    :param skip_leading_rows: Optional number of rows to skip at the top the Google Sheet data range.
    :param where_sql: Optional SQL to insert in the import query's `WHERE` clause to filter the data.
    :param column_transforms: Optional Google Sheet column transformation specifications.
    """
    # Use credentials that include a Google Drive scope.
    credentials, project = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive",
        ]
    )
    client = bigquery.Client(credentials=credentials, project=project)

    external_table_schema = Schema.from_json(deepcopy(destination_schema.schema))
    if column_transforms:
        for column_transform in column_transforms:
            if column_transform.sheet_column_type:
                for field in external_table_schema.schema["fields"]:
                    if field["name"] == column_transform.column:
                        field["type"] = column_transform.sheet_column_type
                        break

    external_table = bigquery.ExternalConfig(
        bigquery.ExternalSourceFormat.GOOGLE_SHEETS
    )
    external_table.source_uris = [sheet_url]
    external_table.schema = external_table_schema.to_bigquery_schema()
    external_table_options = cast(bigquery.GoogleSheetsOptions, external_table.options)
    if sheet_range:
        external_table_options.range = sheet_range
    if skip_leading_rows:
        external_table_options.skip_leading_rows = skip_leading_rows

    job_config = bigquery.QueryJobConfig()
    job_config.destination = destination_table
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.table_definitions = {"google_sheet": external_table}

    select_sql = "*"
    if column_transforms:
        select_sql = (
            "* REPLACE ("
            + ", ".join(
                f"{column_transform.sql} AS {column_transform.column}"
                for column_transform in column_transforms
            )
            + ")"
        )

    result = client.query_and_wait(
        dedent(
            f"""
            SELECT
              {select_sql}
            FROM
              google_sheet
            WHERE
              {where_sql or "TRUE"}
            """
        ),
        job_config=job_config,
    )
    print(
        f"Imported {result.total_rows} rows from {sheet_url} into `{destination_table}`."
    )
