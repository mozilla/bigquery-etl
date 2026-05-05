"""This file contains custom filters for formatting data types in Jinja templates."""

from datetime import datetime, timedelta

from bigquery_etl import query_scheduling
from bigquery_etl.query_scheduling.utils import TIMEDELTA_RE


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


# based on https://stackoverflow.com/questions/4628122/how-to-construct-a
# -timedelta-object-from-a-simple-string
def format_timedelta(timedelta_string):
    """Format a timedelta object."""
    parts = TIMEDELTA_RE.match(timedelta_string)
    if not parts:
        return timedelta_string

    parts = parts.groupdict()
    is_negative = timedelta_string.startswith("-")
    time_params = {}
    for name, param in parts.items():
        if param:
            time_params[name] = int(param) * (-1 if is_negative else 1)

    return timedelta(**time_params)


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
