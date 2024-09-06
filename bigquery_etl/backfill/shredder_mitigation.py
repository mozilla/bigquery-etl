"""Generate a query with shredder mitigation."""

import os
import re
from datetime import date
from datetime import datetime as dt
from datetime import time, timedelta
from enum import Enum
from pathlib import Path
from types import NoneType
from typing import Any, Optional, Tuple

import attr
import click
from dateutil import parser
from gcloud.exceptions import NotFound  # type: ignore
from google.cloud import bigquery
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.metadata.parse_metadata import METADATA_FILE, Metadata
from bigquery_etl.util.common import extract_last_group_by_from_query, write_sql

PREVIOUS_DATE = (dt.now() - timedelta(days=2)).date()
SUFFIX = dt.now().strftime("%Y%m%d%H%M%S")
TEMP_DATASET = "tmp"
THIS_PATH = Path(os.path.dirname(__file__))
DEFAULT_PROJECT_ID = "moz-fx-data-shared-prod"
QUERY_WITH_MITIGATION_NAME = "query_with_shredder_mitigation"


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
    """Data types in BigQuery. Not supported/expected in aggregates: TIMESTAMP, ARRAY, STRUCT."""

    STRING = ("STRING", "BYTES")
    BOOLEAN = "BOOLEAN"
    INTEGER = ("INTEGER", "INT64", "INT", "SMALLINT", "TINYINT", "BYTEINT")
    NUMERIC = (
        "NUMERIC",
        "BIGNUMERIC",
    )
    FLOAT = "FLOAT"
    DATE = "DATE"
    DATETIME = "DATETIME"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    UNDETERMINED = "None"


@attr.define(eq=True)
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


@attr.define(eq=True)
class Subset:
    """Representation of a subset/CTEs in the query and the actions related to this subset."""

    client: bigquery.Client
    destination_table: str = attr.field(default="")
    query_cte: str = attr.field(default="")
    dataset: str = attr.field(default=TEMP_DATASET)
    project_id: str = attr.field(default=DEFAULT_PROJECT_ID)
    expiration_days: Optional[float] = attr.field(default=None)

    @property
    def expiration_ms(self) -> Optional[float]:
        """Convert partition expiration from days to milliseconds."""
        if self.expiration_days is None:
            return None
        return int(self.expiration_days * 86_400_000)

    @property
    def version(self):
        """Return the version of the destination table."""
        match = re.search(r"v(\d+)$", self.destination_table)
        try:
            version = int(match.group(1))
            if not isinstance(version, int):
                raise click.ClickException(
                    f"{self.destination_table} must end with a positive integer."
                )
            return version
        except (AttributeError, TypeError):
            raise click.ClickException(
                f"Invalid or missing table version in {self.destination_table}."
            )

    @property
    def full_table_id(self):
        """Return the full id of the destination table."""
        return f"{self.project_id}.{self.dataset}.{self.destination_table}"

    @property
    def query_path(self):
        """Return the full path of the query.sql file associated with the subset."""
        sql_path = (
            Path("sql")
            / self.project_id
            / self.dataset
            / self.destination_table
            / "query.sql"
        )
        if not os.path.isfile(sql_path):
            click.echo(
                click.style(f"Required file not found: {sql_path}.", fg="yellow")
            )
            return None
        return sql_path

    @property
    def partitioning(self):
        """Return the partition details of the destination table."""
        metadata = Metadata.from_file(
            Path("sql")
            / self.project_id
            / self.dataset
            / self.destination_table
            / METADATA_FILE
        )
        if metadata.bigquery and metadata.bigquery.time_partitioning:
            partitioning = {
                "type": metadata.bigquery.time_partitioning.type.name,
                "field": metadata.bigquery.time_partitioning.field,
            }
        else:
            partitioning = {"type": None, "field": None}
        return partitioning

    def generate_query(
        self,
        select_list,
        from_clause,
        where_clause=None,
        group_by_clause=None,
        order_by_clause=None,
        having_clause=None,
    ):
        """Build query to populate the table."""
        if not select_list or not from_clause:
            raise click.ClickException(
                f"Missing required clause to generate query.\n"
                f"Actuals: SELECT: {select_list}, FROM: {self.full_table_id}"
            )
        query = f"SELECT {', '.join(map(str, select_list))}"
        query += f" FROM {from_clause}" if from_clause is not None else ""
        query += f" WHERE {where_clause}" if where_clause is not None else ""
        query += f" GROUP BY {group_by_clause}" if group_by_clause is not None else ""
        query += (
            f" HAVING {having_clause}"
            if having_clause is not None and group_by_clause is not None
            else ""
        )
        query += f" ORDER BY {order_by_clause}" if order_by_clause is not None else ""
        return query

    def get_query_path_results(
        self,
        backfill_date: date = PREVIOUS_DATE,
        row_limit: Optional[int] = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """Run the query in sql_path & return result or number of rows requested."""
        having_clause = None
        for key, value in kwargs.items():
            if key.lower() == "having_clause":
                having_clause = f"{value}"

        with open(self.query_path, "r") as file:
            sql_text = file.read().strip()

        if sql_text.endswith(";"):
            sql_text = sql_text[:-1]
        if having_clause:
            sql_text = f"{sql_text} {having_clause}"
        if row_limit:
            sql_text = f"{sql_text} LIMIT {row_limit}"

        partition_field = self.partitioning["field"]
        partition_type = (
            "DATE" if self.partitioning["type"] == "DAY" else self.partitioning["type"]
        )
        parameters = None
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
        except NotFound as e:
            raise click.ClickException(
                f"Unable to query data for {backfill_date}. Table {self.full_table_id} not found."
            ) from e
        rows = [dict(row) for row in query_results]
        return rows

    def compare_current_and_previous_version(
        self,
        date_partition_parameter,
    ):
        """Generate and run a data check to compare existing and backfilled data."""
        return NotImplemented


def get_bigquery_type(value) -> DataTypeGroup:
    """Find the datatype of a value, grouping similar types."""
    if isinstance(value, dt):
        return DataTypeGroup.DATETIME
    try:
        if isinstance(dt.strptime(value, "%H:%M:%S").time(), time):
            return DataTypeGroup.TIME
    except (ValueError, TypeError, AttributeError):
        pass
    try:
        value_parsed = parser.isoparse(value.replace(" UTC", "Z").replace(" ", "T"))
        if (
            isinstance(value_parsed, dt)
            and value_parsed.time() == time(0, 0)
            and isinstance(dt.strptime(value, "%Y-%m-%d"), date)
        ):
            return DataTypeGroup.DATE
        if isinstance(value_parsed, dt) and value_parsed.tzinfo is None:
            return DataTypeGroup.DATETIME
        if isinstance(value_parsed, dt) and value_parsed.tzinfo is not None:
            return DataTypeGroup.TIMESTAMP
    except (ValueError, TypeError, AttributeError):
        pass
    if isinstance(value, time):
        return DataTypeGroup.TIME
    if isinstance(value, date):
        return DataTypeGroup.DATE
    if isinstance(value, bool):
        return DataTypeGroup.BOOLEAN
    if isinstance(value, int):
        return DataTypeGroup.INTEGER
    if isinstance(value, float):
        return DataTypeGroup.FLOAT
    if isinstance(value, (str, bytes)):
        return DataTypeGroup.STRING
    if isinstance(value, NoneType):
        return DataTypeGroup.UNDETERMINED
    raise ValueError(f"Unsupported data type: {type(value)}")


def classify_columns(
    new_row: dict, existing_dimension_columns: list, new_dimension_columns: list
) -> tuple[list[Column], list[Column], list[Column], list[Column], list[Column]]:
    """Compare new row with existing columns & return common, added & removed columns."""
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
            f"Existing dimensions don't match columns retrieved by query."
            f" Missing {missing_dimensions}."
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
                value_type is DataTypeGroup.INTEGER
                or value_type is DataTypeGroup.FLOAT
                or value_type is DataTypeGroup.NUMERIC
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
) -> Tuple[Path, str]:
    """Generate a query to backfill with shredder mitigation."""
    query_with_mitigation_path = Path("sql") / project_id

    # Find query files and grouping of previous and new queries.
    new = Subset(client, destination_table, "new_version", dataset, project_id, None)
    if new.version < 2:
        raise click.ClickException(
            f"The new version of the table is expected >= 2. Actual is {new.version}."
        )

    destination_table_previous_version = (
        f"{destination_table[:-len(str(new.version))]}{new.version-1}"
    )
    previous = Subset(
        client,
        destination_table_previous_version,
        "previous",
        dataset,
        project_id,
        None,
    )
    new_group_by = extract_last_group_by_from_query(sql_path=new.query_path)
    previous_group_by = extract_last_group_by_from_query(sql_path=previous.query_path)

    # Check that previous query exists and GROUP BYs are valid in both queries.
    integers_in_group_by = False
    for e in previous_group_by + new_group_by:
        try:
            int(e)
            integers_in_group_by = True
        except ValueError:
            continue
    if (
        "ALL" in previous_group_by
        or "ALL" in new_group_by
        or not all(isinstance(e, str) for e in previous_group_by)
        or not all(isinstance(e, str) for e in new_group_by)
        or not previous_group_by
        or not new_group_by
        or integers_in_group_by
    ):
        raise click.ClickException(
            "GROUP BY must use an explicit list of columns. "
            "Avoid expressions like `GROUP BY ALL` or `GROUP BY 1, 2, 3`."
        )

    # Identify columns common to both queries and columns new. This excludes removed columns.
    sample_rows = new.get_query_path_results(
        backfill_date=backfill_date,
        row_limit=1,
        having_clause=f"HAVING {' IS NOT NULL AND '.join(new_group_by)} IS NOT NULL",
    )
    if not sample_rows:
        sample_rows = new.get_query_path_results(
            backfill_date=backfill_date, row_limit=1
        )
    try:
        new_table_row = sample_rows[0]
        (
            common_dimensions,
            added_dimensions,
            removed_dimensions,
            metrics,
            undetermined_columns,
        ) = classify_columns(new_table_row, previous_group_by, new_group_by)
    except TypeError as e:
        raise click.ClickException(
            f"Table {destination_table} did not return any rows for {backfill_date}.\n{e}"
        )

    if not common_dimensions or not added_dimensions or not metrics:
        raise click.ClickException(
            "The process requires that previous & query have at least one dimension in common,"
            " one dimension added and one metric."
        )

    # Get the new query.
    with open(new.query_path, "r") as file:
        new_query = file.read().strip()

    # Aggregate previous data and new query results using common dimensions.
    new_agg = Subset(
        client, destination_table, "new_agg", TEMP_DATASET, project_id, None
    )
    previous_agg = Subset(
        client,
        destination_table_previous_version,
        "previous_agg",
        TEMP_DATASET,
        project_id,
        None,
    )
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
                and dim.data_type in (DataTypeGroup.INTEGER, DataTypeGroup.FLOAT)
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
        from_clause=f"{new.query_cte}",
        group_by_clause="ALL",
    )
    previous_agg_query = previous_agg.generate_query(
        select_list=common_select,
        from_clause=f"`{previous.full_table_id}`",
        where_clause=f"{previous.partitioning['field']} = @{previous.partitioning['field']}",
        group_by_clause="ALL",
    )

    # Calculate shredder impact.
    shredded = Subset(
        client, destination_table, "shredded", TEMP_DATASET, project_id, None
    )

    # Set values to NULL for the supported types.
    shredded_select = (
        [f"{previous_agg.query_cte}.{new.partitioning['field']}"]
        + [
            f"{previous_agg.query_cte}.{dim.name}"
            for dim in common_dimensions
            if (dim.name != new.partitioning["field"])
        ]
        + [
            f"CAST(NULL AS {DataTypeGroup.STRING.name}) AS {dim.name}"
            for dim in added_dimensions
            if (
                dim.name != new.partitioning["field"]
                and dim.data_type == DataTypeGroup.STRING
            )
        ]
        + [
            f"CAST(NULL AS {DataTypeGroup.BOOLEAN.name}) AS {dim.name}"
            for dim in added_dimensions
            if (
                dim.name != new.partitioning["field"]
                and dim.data_type == DataTypeGroup.BOOLEAN
            )
        ]
        # This doesn't convert data or dtypes, it's only used to cast NULLs for UNION queries.
        + [
            f"CAST(NULL AS {DataTypeGroup.DATE.name}) AS {dim.name}"
            for dim in added_dimensions
            if (
                dim.name != new.partitioning["field"]
                and dim.data_type == DataTypeGroup.DATE
            )
        ]
        + [
            f"CAST(NULL AS {DataTypeGroup.INTEGER.name}) AS {dim.name}"
            for dim in added_dimensions
            if (
                dim.name != new.partitioning["field"]
                and dim.data_type == DataTypeGroup.INTEGER
            )
        ]
        + [
            f"CAST(NULL AS {DataTypeGroup.FLOAT.name}) AS {dim.name}"
            for dim in added_dimensions
            if (
                dim.name != new.partitioning["field"]
                and dim.data_type == DataTypeGroup.FLOAT
            )
        ]
        + [
            f"NULL AS {dim.name}"
            for dim in added_dimensions
            if (
                dim.name != new.partitioning["field"]
                and dim.data_type
                not in (
                    DataTypeGroup.STRING,
                    DataTypeGroup.BOOLEAN,
                    DataTypeGroup.INTEGER,
                    DataTypeGroup.FLOAT,
                )
            )
        ]
        + [
            f"{previous_agg.query_cte}.{metric.name} - IFNULL({new_agg.query_cte}.{metric.name}, 0)"
            f" AS {metric.name}"
            for metric in metrics
            if metric.data_type != DataTypeGroup.FLOAT
        ]
        + [
            f"ROUND({previous_agg.query_cte}.{metric.name}, 3) - "
            f"ROUND(IFNULL({new_agg.query_cte}.{metric.name}, 0), 3) AS {metric.name}"
            for metric in metrics
            if metric.data_type == DataTypeGroup.FLOAT
        ]
    )

    shredded_join = " AND ".join(
        [
            f"{previous_agg.query_cte}.{previous.partitioning['field']} ="
            f" {new_agg.query_cte}.{new.partitioning['field']}"
        ]
        + [
            f"{previous_agg.query_cte}.{dim.name} = {new_agg.query_cte}.{dim.name}"
            for dim in common_dimensions
            if dim.name != previous.partitioning["field"]
            and dim.data_type not in (DataTypeGroup.BOOLEAN, DataTypeGroup.DATE)
        ]
        + [
            f"({previous_agg.query_cte}.{dim.name} = {new_agg.query_cte}.{dim.name} OR"
            f" ({previous_agg.query_cte}.{dim.name} IS NULL"
            f" AND {new_agg.query_cte}.{dim.name} IS NULL))"  # Compare null values.
            for dim in common_dimensions
            if dim.name != previous.partitioning["field"]
            and dim.data_type in (DataTypeGroup.BOOLEAN, DataTypeGroup.DATE)
        ]
    )
    shredded_query = shredded.generate_query(
        select_list=shredded_select,
        from_clause=f"{previous_agg.query_cte} LEFT JOIN {new_agg.query_cte} ON {shredded_join} ",
        where_clause=" OR ".join(
            [
                f"{previous_agg.query_cte}.{metric.name} > IFNULL({new_agg.query_cte}.{metric.name}, 0)"
                for metric in metrics
            ]
        ),
    )

    combined_list = (
        [dim.name for dim in common_dimensions]
        + [dim.name for dim in added_dimensions]
        + [metric.name for metric in metrics]
    )
    final_select = f"{', '.join(combined_list)}"

    click.echo(
        click.style(
            f"""Generating query with shredder mitigation and the following columns:
            Dimensions in both versions:
            {[f"{dim.name}:{dim.data_type.name}" for dim in common_dimensions]},
            Dimensions added:
            {[f"{dim.name}:{dim.data_type.name}" for dim in added_dimensions]}
            Metrics:
            {[f"{dim.name}:{dim.data_type.name}" for dim in metrics]},
            Colums that could not be classified:
            {[f"{dim.name}:{dim.data_type.name}" for dim in undetermined_columns]}.""",
            fg="yellow",
        )
    )

    # Generate query from template.
    env = Environment(loader=FileSystemLoader(str(THIS_PATH)))
    query_with_mitigation_template = env.get_template(
        f"{QUERY_WITH_MITIGATION_NAME}_template.sql"
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
        basename=f"{QUERY_WITH_MITIGATION_NAME}.sql",
        sql=query_with_mitigation_sql,
        skip_existing=False,
    )

    # return Path("sql")
    return (
        Path("sql")
        / new.project_id
        / new.dataset
        / new.destination_table
        / f"{QUERY_WITH_MITIGATION_NAME}.sql",
        query_with_mitigation_sql,
    )
