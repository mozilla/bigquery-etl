from datetime import datetime
import re


def is_timedelta_string(s):
    """
    Checks whether a provided string is in a timedelta format.
    Timedeltas in configs are specified like: 1h, 30m, 1h15m, ...
    """
    timedelta_regex = re.compile(r"(\d+h)?(\d+m)?(\d+s)?")
    return timedelta_regex.match(s)


def is_date_string(s):
    """
    Checks whether a provided string is a valid date string formatted like YYYY-MM-DD. 
    """
    try:
        datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        return False

    return True


def is_email(s):
    """Checks whether the provided string is a valid email address."""
    # https://stackoverflow.com/questions/8022530/how-to-check-for-valid-email-address
    return re.match(r"[^@]+@[^@]+\.[^@]+", s)
