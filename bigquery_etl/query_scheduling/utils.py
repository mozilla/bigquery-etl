"""Utility functions for scheduling queries."""

from datetime import datetime
import re


def is_timedelta_string(s):
    """
    Check whether a provided string is in a timedelta format.

    Timedeltas in configs are specified like: 1h, 30m, 1h15m, ...
    """
    timedelta_regex = re.compile(r"^(\d+h)?(\d+m)?(\d+s)?$")
    return timedelta_regex.match(s)


def is_date_string(s):
    """Check whether a string is a valid date string formatted like YYYY-MM-DD."""
    try:
        datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        return False

    return True


def is_email(s):
    """Check whether the provided string is a valid email address."""
    # https://stackoverflow.com/questions/8022530/how-to-check-for-valid-email-address
    return re.match(r"[^@]+@[^@]+\.[^@]+", s)


DAG_NAME_RE = re.compile("^bqetl_.+$")


def is_valid_dag_name(name):
    """Check whether the DAG name is valid."""
    return DAG_NAME_RE.match(name)


# https://stackoverflow.com/questions/14203122/create-a-regular-expression-for-cron-statement
SCHEDULE_INTERVAL_RE = re.compile(
    r"^(once|hourly|daily|weekly|monthly|yearly|"
    r"((((\d+,)+\d+|(\d+(\/|-)\d+)|\d+|\*) ?){5,7})|"
    r"((\d+h)?(\d+m)?(\d+s)?))$"
)


def is_schedule_interval(interval):
    """
    Check whether the provided string is a valid schedule interval.

    Schedule intervals can be either in CRON format or one of:
    @once, @hourly, @daily, @weekly, @monthly, @yearly
    or a timedelta []d[]h[]m
    """
    return SCHEDULE_INTERVAL_RE.match(interval)
