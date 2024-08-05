"""Generate a query to backfill an aggregate with shredder mitigation."""

from datetime import date, datetime, time, timedelta
from enum import Enum
from types import NoneType

import click

TEMP_DATASET = "tmp"
SUFFIX = datetime.now().strftime("%Y%m%d%H%M%S")
PREVIOUS_DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


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


class Column:
    """Representation of a column in a query, with relevant details for shredder mitigation."""

    def __init__(self, name, data_type, column_type, status):
        """Initialize class with required attributes."""
        self.name = name
        self.data_type = data_type
        self.column_type = column_type
        self.status = status

    def __eq__(self, other):
        """Return attributes only if the referenced object is of type Column."""
        if not isinstance(other, Column):
            return NotImplemented
        return (
            self.name == other.name
            and self.data_type == other.data_type
            and self.column_type == other.column_type
            and self.status == other.status
        )

    def __repr__(self):
        """Return a string representation of the object."""
        return f"Column(name={self.name}, data_type={self.data_type}, column_type={self.column_type}, status={self.status})"


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
            f"Missing required parameters. Received: new_row= {new_row}\n"
            f"existing_dimension_columns= {existing_dimension_columns},\nnew_dimension_columns= {new_dimension_columns}."
        )

    missing_dimensions = [
        dimension for dimension in new_dimension_columns if dimension not in new_row
    ]
    if not len(missing_dimensions) == 0:
        raise click.ClickException(
            f"Inconsistent parameters. Columns in new dimensions not found in new row: {missing_dimensions}"
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
