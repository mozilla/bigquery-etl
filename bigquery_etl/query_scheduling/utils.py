"""Utility functions for scheduling queries."""

from datetime import datetime
import re


def is_timedelta_string(s):
    """
    Check whether a provided string is in a timedelta format.

    Timedeltas in configs are specified like: 1h, 30m, 1h15m, ...
    """
    timedelta_regex = re.compile(r"^-?(\d+h)?(\d+m)?(\d+s)?$")
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


def schedule_interval_delta(schedule_interval1, schedule_interval2):
    """Return the time delta between two schedule intervals as timedelta string."""
    if not is_schedule_interval(schedule_interval1) or not is_schedule_interval(
        schedule_interval2
    ):
        return None

    aliases = {
        "yearly": "0 0 1 1 *",
        "monthly": "0 0 1 * *",
        "weekly": "0 0 * * 0",
        "daily": "0 0 * * *",
        "hourly": "0 * * * *",
        "once": "* * * * *",
    }

    cron_regex = re.compile(
        r"^(?P<minutes>\d+) (?P<hours>\d+) (?P<day>\d+) (?P<month>\d+) (?P<dow>\d+)$"
    )

    if schedule_interval1 in aliases:
        schedule_interval1 = aliases[schedule_interval1]

    if schedule_interval2 in aliases:
        schedule_interval2 = aliases[schedule_interval2]

    si1 = schedule_interval1.replace("*", "0")
    si2 = schedule_interval2.replace("*", "0")

    if cron_regex.match(si2) is None or cron_regex.match(si1) is None:
        return None

    parts1 = cron_regex.match(si1).groupdict()
    parts2 = cron_regex.match(si2).groupdict()
    # delta in seconds
    delta = 0
    delta += (int(parts2["hours"]) - int(parts1["hours"])) * 60 * 60
    delta += (int(parts2["minutes"]) - int(parts1["minutes"])) * 60
    delta += (int(parts2["day"]) - int(parts1["day"])) * 24 * 60 * 60

    # todo handle month and day of week

    return f"{delta}s"
