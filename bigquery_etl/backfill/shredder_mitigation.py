"""Generate a query to backfill an aggregate with shredder mitigation."""
import os
import re
from datetime import date, datetime, time, timedelta
from enum import Enum
from pathlib import Path
from types import NoneType
from typing import Optional

import attr
import click
from gcloud.exceptions import NotFound
from google.cloud import bigquery
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import (
    extract_last_group_by_from_query,
    paths_matching_name_pattern,
)
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.metadata.parse_metadata import METADATA_FILE, Metadata, PartitionType
from bigquery_etl.util.common import write_sql

PREVIOUS_DATE = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
SUFFIX = datetime.now().strftime("%Y%m%d%H%M%S")
TEMP_DATASET = "tmp"
THIS_PATH = Path(os.path.dirname(__file__))
DEFAULT_PROJECT_ID = "moz-fx-data-shared-prod"


class ColumnType(Enum):
    """Differentiate metric and dimensions."""

    METRIC = "METRIC"
    DIMENSION = "DIMENSION"
    UNDETERMINED = "None"


class ColumnStatus(Enum):
    """Different status of a column during shredder mitigation."""

    COMMON = "COMMON"
    ADDED = "ADDED"
    REMOVED = "REMOVED"
    UNDETERMINED = "None"


class DataTypeGroup(Enum):
    """Data types in BigQuery. Not including ARRAY and STRUCT as these are not expected in aggregate tables."""

    STRING = ("STRING", "BYTES")
    BOOLEAN = "BOOLEAN"
    NUMERIC = (
        "INTEGER",
        "NUMERIC",
        "BIGNUMERIC",
        "DECIMAL",
        "INT64",
        "INT",
        "SMALLINT",
        "BIGINT",
        "TINYINT",
        "BYTEINT",
    )
    FLOAT = ("FLOAT",)
    DATE = ("DATE", "DATETIME", "TIME", "TIMESTAMP")
    UNDETERMINED = "None"


@attr.define
class Column:
    """Representation of a column in a query, with relevant details for shredder mitigation."""

    name: str
    data_type: DataTypeGroup = attr.field(default=DataTypeGroup.UNDETERMINED)
    column_type: ColumnType = attr.field(default=ColumnType.UNDETERMINED)
    status: ColumnStatus = attr.field(default=ColumnStatus.UNDETERMINED)

    """Validate the type of the attributes."""

    @data_type.validator
    def validate_data_type(self, attribute, value):
        """Check that the type of data_type is as expected."""
        if not isinstance(value, DataTypeGroup):
            raise ValueError(f"Invalid {value} with type: {type(value)}.")

    @column_type.validator
    def validate_column_type(self, attribute, value):
        """Check that the type of parameter column_type is as expected."""
        if not isinstance(value, ColumnType):
            raise ValueError(f"Invalid data type for: {value}.")

    @status.validator
    def validate_status(self, attribute, value):
        """Check that the type of parameter column_status is as expected."""
        if not isinstance(value, ColumnStatus):
            raise ValueError(f"Invalid data type for: {value}.")


@attr.define
class Subset:
    client: bigquery.Client()
    destination_table: str = attr.field()
    suffix: str = attr.field(converter=attr.converters.default_if_none(""))
    query_cte: str = attr.field(converter=attr.converters.default_if_none(""))
    dataset: str = attr.field(default=TEMP_DATASET)
    project_id: str = attr.field(default=DEFAULT_PROJECT_ID)
    expiration_days: Optional[float] = attr.field(default=None)

    @destination_table.validator
    def validate_table_version(self, attribute, value):
        """Raise an exception if the version table name doesn't end with an integer."""
        match = re.search(r"v(\d+)$", self.destination_table)
        try:
            version = int(match.group(1))
            if not isinstance(version, int) or version <= 0:
                raise click.ClickException(
                    f"Destination table {value} must end with a positive version number."
                )
        except AttributeError:
            f"Invalid table version for {value}"

    @property
    def expiration_ms(self) -> Optional[float]:
        """Convert partition expiration from days to milliseconds."""
        if self.expiration_days is None:
            return None
        return int(self.expiration_days * 86400000)

    @property
    def name(self):
        if self.suffix is not None and self.suffix != "":
            return f"{self.destination_table}{self.suffix}"
        else:
            return self.destination_table

    @property
    def version(self):
        match = re.search(r"v(\d+)$", self.destination_table)
        return int(match.group(1))

    @property
    def name_previous_version(self):
        """Return the table name for the previous version of the table."""
        return f"{self.destination_table[:-1]}{int(self.version)-1}"

    @property
    def full_table_id(self):
        return f"{self.project_id}.{self.dataset}.{self.name}"

    @property
    def partitioning(self) -> str:
        metadata = Metadata.from_file(
            Path("sql") / self.project_id / self.dataset / self.name / METADATA_FILE
        )
        if metadata.bigquery and metadata.bigquery.time_partitioning:
            partitioning = {
                "type": metadata.bigquery.time_partitioning.type,
                "field": metadata.bigquery.time_partitioning.field,
            }
        return partitioning

    def generate_query(
        self,
        select_list,
        from_clause=None,
        where_clause=None,
        group_by_clause=None,
        order_by_clause=None,
    ):
        """Build query to populate the table."""

        query = f"SELECT {', '.join(select_list)}"
        query += f" {from_clause}" if from_clause is not None else ""
        query += f" {where_clause}" if where_clause is not None else ""
        query += f" {group_by_clause}" if group_by_clause is not None else ""
        query += f" {order_by_clause};" if order_by_clause is not None else ""
        return query

    def get_query_path(self):
        """Return the path of the query file for this subset."""
        query_files = paths_matching_name_pattern(
            f"{self.dataset}.{self.name}", "sql", self.project_id
        )
        if query_files == []:
            raise click.ClickException(
                f"Query file not found for `{self.full_table_id}`."
            )
        return query_files[0]

    def get_query_path_results(
        self, backfill_date: date = PREVIOUS_DATE, rows: int = None, **kwargs
    ) -> list[{}]:
        """Run the query available in the sql_path and return the result or the number of rows requested."""

        having_clause = None
        for key, value in kwargs.items():
            if key.lower() == "having_clause":
                having_clause = f"{value}"

        with open(self.get_query_path(), "r") as file:
            sql_text = file.read().strip()

        if sql_text.endswith(";"):
            sql_text = sql_text[:-1]
        if having_clause:
            sql_text = f"{sql_text} {having_clause}"
        if rows:
            sql_text = f"{sql_text} LIMIT {rows};"

        partition_field = self.partitioning["field"]
        partition_type = (
            "DATE"
            if self.partitioning["type"] == PartitionType.DAY
            else self.partitioning["type"]
        )
        if partition_field is not None:
            parameters = [
                bigquery.ScalarQueryParameter(
                    partition_field, partition_type, backfill_date
                ),
            ]

        try:
            query_job = self.client.query(
                query=sql_text,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=parameters,
                    use_legacy_sql=False,
                    dry_run=False,
                    use_query_cache=False,
                ),
            )
            query_results = query_job.result()
        except NotFound:
            raise click.ClickException(
                f"Unable to query current data, table {self.full_table_id} doesn't exist."
            )
        rows = [dict(row) for row in query_results]
        return rows

    def compare_current_and_previous_version(
        self,
        date_partition_parameter,
        backfill_date_str,
        columns_to_compare,
        metrics_to_compare,
    ):
        """Generate and run a data check to compare existing and backfilled data."""
        return NotImplemented


def get_bigquery_type(value) -> DataTypeGroup:
    """Return the datatype of a value, grouping similar types."""
    date_formats = [
        ("%Y-%m-%d", date),
        ("%Y-%m-%d %H:%M:%S", datetime),
        ("%Y-%m-%dT%H:%M:%S", datetime),
        ("%Y-%m-%dT%H:%M:%SZ", datetime),
        ("%Y-%m-%d %H:%M:%S UTC", datetime),
        ("%H:%M:%S", time),
    ]
    for format, dtype in date_formats:
        try:
            if dtype == time:
                parsed_to_time = datetime.strptime(value, format).time()
                if isinstance(parsed_to_time, time):
                    return DataTypeGroup.DATE
            else:
                parsed_to_date = datetime.strptime(value, format)
                if isinstance(parsed_to_date, dtype):
                    return DataTypeGroup.DATE
        except (ValueError, TypeError):
            continue
    if isinstance(value, time):
        return DataTypeGroup.DATE
    if isinstance(value, date):
        return DataTypeGroup.DATE
    elif isinstance(value, bool):
        return DataTypeGroup.BOOLEAN
    if isinstance(value, int):
        return DataTypeGroup.NUMERIC
    elif isinstance(value, float):
        return DataTypeGroup.FLOAT
    elif isinstance(value, str) or isinstance(value, bytes):
        return DataTypeGroup.STRING
    elif isinstance(value, NoneType):
        return DataTypeGroup.UNDETERMINED
    else:
        raise ValueError(f"Unsupported data type: {type(value)}")


def classify_columns(
    new_row: dict, existing_dimension_columns: list, new_dimension_columns: list
) -> tuple[list[Column], list[Column], list[Column], list[Column], list[Column]]:
    """Compare the new row with the existing columns and return the list of common, added and removed columns by type."""
    common_dimensions = []
    added_dimensions = []
    removed_dimensions = []
    metrics = []
    undefined = []

    if not new_row or not existing_dimension_columns or not new_dimension_columns:
        raise click.ClickException(
            f"\n\nMissing one or more required parameters. Received:"
            f"\nnew_row= {new_row}"
            f"\nexisting_dimension_columns= {existing_dimension_columns},"
            f"\nnew_dimension_columns= {new_dimension_columns}."
        )

    missing_dimensions = [
        dimension for dimension in new_dimension_columns if dimension not in new_row
    ]
    if not len(missing_dimensions) == 0:
        raise click.ClickException(
            f"Inconsistent parameters. New dimension columns not found in new row: {missing_dimensions}"
            f"\n new_dimension_columns: {new_dimension_columns}"
            f"\n new_row: {new_row}"
        )

    for key in existing_dimension_columns:
        if key not in new_dimension_columns:
            removed_dimensions.append(
                Column(
                    key,
                    DataTypeGroup.UNDETERMINED,
                    ColumnType.UNDETERMINED,
                    ColumnStatus.REMOVED,
                )
            )

    for key, value in new_row.items():
        value_type = get_bigquery_type(value)
        if key in existing_dimension_columns:
            common_dimensions.append(
                Column(
                    key,
                    value_type,
                    ColumnType.DIMENSION,
                    ColumnStatus.COMMON,
                )
            )
        elif key not in existing_dimension_columns and key in new_dimension_columns:
            added_dimensions.append(
                Column(
                    key,
                    value_type,
                    ColumnType.DIMENSION,
                    ColumnStatus.ADDED,
                )
            )
        elif (
            key not in existing_dimension_columns
            and key not in new_dimension_columns
            and (
                value_type is DataTypeGroup.NUMERIC or value_type is DataTypeGroup.FLOAT
            )
        ):
            # Columns that are not in the previous or new list of grouping columns are metrics.
            metrics.append(
                Column(
                    key,
                    value_type,
                    ColumnType.METRIC,
                    ColumnStatus.COMMON,
                )
            )
        else:
            undefined.append(
                Column(
                    key,
                    value_type,
                    ColumnType.UNDETERMINED,
                    ColumnStatus.UNDETERMINED,
                )
            )

    common_dimensions_sorted = sorted(common_dimensions, key=lambda column: column.name)
    added_dimensions_sorted = sorted(added_dimensions, key=lambda column: column.name)
    removed_dimensions_sorted = sorted(
        removed_dimensions, key=lambda column: column.name
    )
    metrics_sorted = sorted(metrics, key=lambda column: column.name)
    undefined_sorted = sorted(undefined, key=lambda column: column.name)

    return (
        common_dimensions_sorted,
        added_dimensions_sorted,
        removed_dimensions_sorted,
        metrics_sorted,
        undefined_sorted,
    )


def generate_query_with_shredder_mitigation(
    client, project_id, dataset, destination_table, backfill_date=PREVIOUS_DATE
) -> Path:
    """Generate a query to backfill with shredder mitigation."""

    # Find query files and grouping of previous and new 'current' query.
    new = Subset(
        client, destination_table, None, "new_version", dataset, project_id, None
    )
    previous = Subset(
        client, new.name_previous_version, None, "previous", dataset, project_id, None
    )

    query_with_mitigation_name = "query_with_shredder_mitigation"
    query_with_mitigation_path = Path("sql") / project_id

    # Check that previous query exists and GROUP BYs are valid in both queries.
    previous_group_by = extract_last_group_by_from_query(previous.get_query_path())
    new_group_by = extract_last_group_by_from_query(new.get_query_path())
    if (
        "ALL" in previous_group_by
        or "ALL" in new_group_by
        or not all(isinstance(e, str) for e in previous_group_by)
        or not all(isinstance(e, str) for e in new_group_by)
        or not previous_group_by
        or not new_group_by
    ):
        raise click.ClickException(
            f"GROUP BY must use an explicit list of columns, avoid `GROUP BY ALL`."
            f"\nPrevious query returns `GROUP BY {previous_group_by}`."
            f"\nNew query returns `GROUP BY {new_group_by}`."
        )

    # Identify columns common to both versions & added columns. This excludes removed columns.
    having_clause = f"HAVING {' IS NOT NULL AND '.join(new_group_by)} IS NOT NULL"
    sample_rows = new.get_query_path_results(
        backfill_date=backfill_date, rows=1, having_clause=having_clause
    )
    if not sample_rows:
        new_table_row = new.get_query_path_results(backfill_date=backfill_date, rows=1)
    try:
        new_table_row = sample_rows[0]
        (
            common_dimensions,
            added_dimensions,
            removed_dimensions,
            metrics,
            undetermined_columns,
        ) = classify_columns(new_table_row, previous_group_by, new_group_by)
    except TypeError:
        click.echo(
            f"Error. The query {new.name} did not return any rows for the backfill date {backfill_date}."
        )

    # Get new query.
    with open(new.get_query_path(), "r") as file:
        new_query = file.read().strip()

    # Aggregate previous data and new query results using common dimensions.
    new_agg = Subset(client, new.name, None, "new_agg", TEMP_DATASET, project_id, None)
    previous_agg = Subset(
        client, previous.name, None, "previous_agg", TEMP_DATASET, project_id, None
    )
    # select_list=[previous.partitioning["field"]]+
    common_select = (
        [previous.partitioning["field"]]
        + [
            f"COALESCE({dim.name}, '??') AS {dim.name}"
            for dim in common_dimensions
            if (
                dim.name != previous.partitioning["field"]
                and dim.data_type == DataTypeGroup.STRING
            )
        ]
        + [
            f"COALESCE({dim.name}, -999) AS {dim.name}"
            for dim in common_dimensions
            if (
                dim.name != new.partitioning["field"]
                and dim.data_type == DataTypeGroup.NUMERIC
            )
        ]
        + [
            f"COALESCE({dim.name}, -999) AS {dim.name}"
            for dim in common_dimensions
            if (
                dim.name != new.partitioning["field"]
                and dim.data_type == DataTypeGroup.FLOAT
            )
        ]
        + [
            dim.name
            for dim in common_dimensions
            if (
                dim.name != new.partitioning["field"]
                and dim.data_type in (DataTypeGroup.BOOLEAN, DataTypeGroup.DATE)
            )
        ]
        + [f"SUM({metric.name}) AS {metric.name}" for metric in metrics]
    )
    new_agg_query = new_agg.generate_query(
        select_list=common_select,
        from_clause=f"FROM {new.query_cte}",
        group_by_clause="GROUP BY ALL",
    )
    previous_agg_query = previous_agg.generate_query(
        select_list=common_select,
        from_clause=f"FROM `{previous.full_table_id}`",
        where_clause=f"WHERE {previous.partitioning['field']} = @{previous.partitioning['field']}",
        group_by_clause="GROUP BY ALL",
    )

    # Calculate shredder impact.
    shredded = Subset(
        client, destination_table, None, "shredded", TEMP_DATASET, project_id, None
    )
    shredded_join = " AND ".join(
        [
            f"{previous_agg.query_cte}.{previous.partitioning['field']} = {new_agg.query_cte}.{new.partitioning['field']}"
        ]
        + [
            f"{previous_agg.query_cte}.{dim.name} = {new_agg.query_cte}.{dim.name}"
            for dim in common_dimensions
            if dim.name != previous.partitioning["field"]
            and dim.data_type not in (DataTypeGroup.BOOLEAN, DataTypeGroup.DATE)
        ]
        + [
            f"({previous_agg.query_cte}.{dim.name} = {new_agg.query_cte}.{dim.name} OR ({previous_agg.query_cte}.{dim.name} IS NULL AND {new_agg.query_cte}.{dim.name} IS NULL))"
            for dim in common_dimensions
            if dim.name != previous.partitioning["field"]
            and dim.data_type in (DataTypeGroup.BOOLEAN, DataTypeGroup.DATE)
        ]
    )
    shredded_query = shredded.generate_query(
        select_list=[
            f"{previous_agg.query_cte}.{dim.name}" for dim in common_dimensions
        ]
        + [f"'??' AS {dim.name}" for dim in added_dimensions]
        + [
            f"{previous_agg.query_cte}.{metric.name} - IFNULL({new_agg.query_cte}.{metric.name}, 0) AS {metric.name}"
            for metric in metrics
            if metric.data_type != DataTypeGroup.FLOAT
        ]
        + [
            f"ROUND({previous_agg.query_cte}.{metric.name}, 3) - ROUND(IFNULL({new_agg.query_cte}.{metric.name}, 0), 3) AS {metric.name}"
            for metric in metrics
            if metric.data_type == DataTypeGroup.FLOAT
        ],
        from_clause=f"FROM {previous_agg.query_cte} LEFT JOIN {new_agg.query_cte} ON {shredded_join} ",
        where_clause=f"WHERE {' OR '.join([f'{previous_agg.query_cte}.{metric.name} > IFNULL({new_agg.query_cte}.{metric.name}, 0)' for metric in metrics])}",
    )

    final_select = f"{', '.join([dim.name for dim in common_dimensions]+[dim.name for dim in added_dimensions]+[metric.name for metric in metrics])}"

    # Generate query from template.
    env = Environment(loader=FileSystemLoader(str(THIS_PATH)))
    query_with_mitigation_template = env.get_template(
        f"{query_with_mitigation_name}_template.sql"
    )

    query_with_mitigation_sql = reformat(
        query_with_mitigation_template.render(
            new_version_cte=new.query_cte,
            new_version=new_query,
            new_agg_cte=new_agg.query_cte,
            new_agg=new_agg_query,
            previous_agg_cte=previous_agg.query_cte,
            previous_agg=previous_agg_query,
            shredded_cte=shredded.query_cte,
            shredded=shredded_query,
            final_select=final_select,
        )
    )

    write_sql(
        output_dir=query_with_mitigation_path,
        full_table_id=new.full_table_id,
        basename=f"{query_with_mitigation_name}.sql",
        sql=query_with_mitigation_sql,
        skip_existing=False,
    )

    return query_with_mitigation_sql
