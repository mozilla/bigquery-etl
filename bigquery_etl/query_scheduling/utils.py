"""Utility functions for scheduling queries."""

import re
from datetime import datetime, timedelta
from typing import Optional

TIMEDELTA_RE = re.compile(
    r"^-?(?=\d)((?P<weeks>\d+)w)?((?P<days>\d+)d)?((?P<hours>\d+)h)?((?P<minutes>\d+)m)?((?P<seconds>\d+)s)?$"
)

TELEMETRY_ALERTS_EMAIL = "telemetry-alerts@mozilla.com"


class InvalidTimedeltaError(ValueError):
    """Raised for invalid timedelta values (i.e. non-strings or in the wrong format)."""

    def __init__(self, value):
        """Initialize the error with the invalid value."""
        super().__init__(
            f"Invalid timedelta value {value!r}."
            " Timedelta values should be strings like '-1d', '1h', '45m', or '1h30m', with the supported units"
            " being weeks (w), days (d), hours (h), minutes (m), and seconds (s), in that order."
        )


def is_timedelta_string(s):
    """
    Check whether a provided value is a string in a timedelta format.

    Timedeltas in configs are specified like: 1h, 30m, 1h15m, ...
    """
    return isinstance(s, str) and TIMEDELTA_RE.match(s) is not None


def validate_timedelta_string(s):
    """Raise an error if the provided string is not in a valid timedelta format."""
    if not is_timedelta_string(s):
        raise InvalidTimedeltaError(s)


def negate_timedelta_string(s: str) -> str:
    """Negate the provided timedelta string."""
    return s[1:] if s.startswith("-") else ("-" + s)


class ReadableTimedelta(timedelta):
    """A timedelta with more human-readable str/repr output."""

    def __str__(self) -> str:
        """Format the timedelta as a more human-readable string.

        For example, a -1 hour value will be formatted as "-1:00:00" instead of "-1 day, 23:00:00".
        """
        if self < timedelta(0):
            # For negative values we also show a non-zero time as negated to avoid confusion with standard `timedelta` output,
            # where negative days and positive time actually means that many hours/minutes/seconds closer to zero.
            return re.sub(r", (?!0:00:00$)", ", -", f"-{abs(self)}")
        return super().__str__()

    def __repr__(self) -> str:
        """Format the timedelta as a more human-readable representation string.

        For example, a -1 hour value will be formatted as "datetime.timedelta(hours=-1)" instead of "datetime.timedelta(days=-1, seconds=82800)".
        """
        if not self:
            return "datetime.timedelta(0)"
        sign = -1 if self < timedelta(0) else 1
        magnitude = abs(self)
        # Internally `timedelta` arbitrarily normalizes values to days/seconds/microseconds,
        # so it requires some calculations to get the hours and minutes back.
        hours, seconds = divmod(magnitude.seconds, 3600)
        minutes, seconds = divmod(seconds, 60)
        units = {
            "days": magnitude.days,
            "hours": hours,
            "minutes": minutes,
            "seconds": seconds,
            "microseconds": magnitude.microseconds,
        }
        arguments = [f"{unit}={sign * value}" for unit, value in units.items() if value]
        return f"datetime.timedelta({', '.join(arguments)})"


# https://stackoverflow.com/questions/4628122/how-to-construct-a-timedelta-object-from-a-simple-string
def parse_timedelta(td: Optional[str | timedelta]) -> Optional[ReadableTimedelta]:
    """Parse a timedelta string into a ReadableTimedelta object, passing through timedelta values and `None`."""
    if isinstance(td, ReadableTimedelta):
        return td
    if isinstance(td, timedelta):
        return ReadableTimedelta(
            days=td.days, seconds=td.seconds, microseconds=td.microseconds
        )
    if td is None or td == "":
        return None
    if not isinstance(td, str):
        raise InvalidTimedeltaError(td)

    match = TIMEDELTA_RE.match(td)
    if not match:
        raise InvalidTimedeltaError(td)

    parts = match.groupdict()
    is_negative = td.startswith("-")
    time_params = {}
    for name, param in parts.items():
        if param:
            time_params[name] = int(param) * (-1 if is_negative else 1)

    return ReadableTimedelta(**time_params)


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


def is_github_identity(s):
    """Check if the given string matches the format of a Github identity."""
    return re.match(r"[@mozilla]+\/[a-zA-Z0-9]+", s)


def is_email_or_github_identity(s):
    """Check if the given string is either an email or a Github identity."""
    return is_email(s) or is_github_identity(s)


def ensure_telemetry_alerts_email(emails: list[str], no_triage: bool) -> list[str]:
    """Ensure the telemetry alerts email is included or excluded depending on the `no_triage` setting."""
    if TELEMETRY_ALERTS_EMAIL not in emails and not no_triage:
        return emails + [TELEMETRY_ALERTS_EMAIL]
    elif TELEMETRY_ALERTS_EMAIL in emails and no_triage:
        return [email for email in emails if email != TELEMETRY_ALERTS_EMAIL]
    return emails


DAG_NAME_RE = re.compile("^(private_)?bqetl_.+$")


def is_valid_dag_name(name):
    """Check whether the DAG name is valid."""
    return DAG_NAME_RE.match(name)


# https://stackoverflow.com/questions/14203122/create-a-regular-expression-for-cron-statement
SCHEDULE_INTERVAL_RE = re.compile(
    r"^(once|hourly|daily|weekly|monthly|yearly|"
    r"((((\d+,)+\d+|(\d+(\/|-)\d+)|\d+|\*/\d+|\*) ?){5,7})|"
    r"((\d+h)?(\d+m)?(\d+s)?))$"
)


def is_schedule_interval(interval):
    """Check whether the provided string is a valid schedule interval."""
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
