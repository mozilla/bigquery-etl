"""
This file contains custom Jinja filters for formatting
certain data types in Jinja templates.
"""

from datetime import datetime, timedelta
import re

from bigquery_etl import query_scheduling
from bigquery_etl.query_scheduling.utils import is_timedelta_string


def format_schedule_interval(interval):
    """Format the input value to a Airflow schedule_interval value."""

    presets = ["once", "hourly", "daily", "weekly", "monthly", "yearly"]

    if interval in presets:
        return "'@" + interval + "'"

    if re.match(r"^((((\d+,)+\d+|(\d+(\/|-)\d+)|\d+|\*) ?){5,7})$", interval):
        # the interval should is a CRON expression
        return "'" + interval + "'"

    return interval


def format_attr(d, attribute, formatter_name):
    """Apply a formatter to a dict attribute."""
    for name in dir(query_scheduling.formatters):
        formatter_func = getattr(query_scheduling.formatters, name)
        if callable(formatter_func) and name == formatter_name:
            if attribute in d:
                d[attribute] = formatter_func(d[attribute])

    return d


def format_date(date_string):
    """Formats a date string to a datetime object."""
    return datetime.strptime(date_string, "%Y-%m-%d")


# based on https://stackoverflow.com/questions/4628122/how-to-construct-a-timedelta-object-from-a-simple-string
def format_timedelta(timdelta_string):
    """Formats a timedelta object."""

    timedelta_regex = re.compile(
        r"^((?P<hours>\d+?)h)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?$"
    )
    parts = timedelta_regex.match(timdelta_string)
    if not parts:
        return timdelta_string

    parts = parts.groupdict()
    time_params = {}
    for (name, param) in parts.items():
        if param:
            time_params[name] = int(param)

    return repr(timedelta(**time_params))


def format_optional_string(val):
    """Formats a value that is either None or a string."""
    if val is None:
        return "None"
    else:
        return "'" + val + "'"
