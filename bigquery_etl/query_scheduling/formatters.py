"""This file contains custom filters for formatting data types in Jinja templates."""

from datetime import datetime
from typing import Optional

from bigquery_etl import query_scheduling
from bigquery_etl.metadata.parse_metadata import PartitionType
from bigquery_etl.query_scheduling.partitioning import resolve_partition_parameter_type
from bigquery_etl.query_scheduling.task import Task
from bigquery_etl.query_scheduling.utils import is_timedelta_string, parse_timedelta


def format_schedule_interval(interval):
    """Format the input value to a Airflow schedule_interval value."""
    presets = ["once", "hourly", "daily", "weekly", "monthly", "yearly"]

    if interval in presets:
        return "'@" + interval + "'"

    return repr(interval)


def format_attr(d, attribute, formatter_name):
    """Apply a formatter to a dict attribute."""
    for name in dir(query_scheduling.formatters):
        if name != formatter_name:
            continue

        formatter_func = getattr(query_scheduling.formatters, name)

        if callable(formatter_func) is None:
            continue

        if attribute in d:
            d[attribute] = formatter_func(d[attribute])

    return d


def format_date(date_string):
    """Format a date string to a datetime object."""
    if date_string is None:
        return None

    return datetime.strptime(date_string, "%Y-%m-%d")


def format_timedelta(timedelta_string):
    """Format a timedelta object."""
    # Pass through other strings like cron schedule intervals.
    if isinstance(timedelta_string, str) and not is_timedelta_string(timedelta_string):
        return timedelta_string
    return parse_timedelta(timedelta_string)


def format_timedelta_macro(timedelta_string):
    """Format an Airflow timedelta macro."""
    timedelta = repr(format_timedelta(timedelta_string))
    return timedelta.replace("datetime", "macros")


def format_optional_string(val):
    """Format a value that is either None or a string."""
    if val is None:
        return "None"
    else:
        return "'" + val + "'"


def format_repr(val):
    """Return representation of a object."""
    return repr(val)


def format_as_tuple(val):
    """Return tuple representation of the provided value."""
    return tuple(val)


def _format_task_partition_base_value(task: Task) -> str:
    if task.partition_offset:
        return f"(logical_date + {format_timedelta_macro(task.partition_offset)})"
    return "logical_date"


def format_task_destination_partition(task: Task) -> Optional[str]:
    """Return the task's destination partition in an Airflow-friendly format like `{{ logical_date.format('YYYYMMDD') }}`."""
    if not task.partition_parameter and not task.partition_offset:
        return None

    partition_base_value = _format_task_partition_base_value(task)

    # https://docs.cloud.google.com/bigquery/docs/partitioned-tables#partition_decorators
    match task.partition_type:
        case PartitionType.YEAR:
            partition_id_format = "YYYY"
        case PartitionType.MONTH:
            partition_id_format = "YYYYMM"
        # If there's no partitioning metadata then assume day partitioning to preserve historical behavior.
        case PartitionType.DAY | None:
            partition_id_format = "YYYYMMDD"
        case PartitionType.HOUR:
            partition_id_format = "YYYYMMDDHH"
        case _:
            raise NotImplementedError(
                f"Unsupported partition type: {task.partition_type}"
            )

    return "{{ " f"{partition_base_value}.format('{partition_id_format}')" " }}"


def format_task_partition_parameter(task: Task) -> Optional[str]:
    """Return the task's partition query parameter in an Airflow-friendly format like `submission_date:DATE:{{ ... }}`."""
    if not task.partition_parameter:
        return None

    partition_base_value = _format_task_partition_base_value(task)
    partition_parameter_type = resolve_partition_parameter_type(task.partition_type)

    match task.partition_type:
        case PartitionType.YEAR:
            partition_parameter_format = "YYYY"
        case PartitionType.MONTH:
            partition_parameter_format = "YYYY-MM-01"
        # If there's no partitioning metadata then assume day partitioning to preserve historical behavior.
        case PartitionType.DAY | None:
            partition_parameter_format = "YYYY-MM-DD"
        case PartitionType.HOUR:
            partition_parameter_format = "YYYY-MM-DD HH:00:00"
        case _:
            raise NotImplementedError(
                f"Unsupported partition type: {task.partition_type}"
            )

    return (
        f"{task.partition_parameter}:{partition_parameter_type}:"
        "{{ "
        f"{partition_base_value}.format('{partition_parameter_format}')"
        " }}"
    )
