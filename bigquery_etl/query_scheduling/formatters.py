"""
This file contains custom Jinja filters for formatting
certain data types in Jinja templates.
"""


def format_schedule_interval(interval):
    """Format the input value to a Airflow schedule_interval value."""

    presets = ["once", "hourly", "daily", "weekly", "monthly", "yearly"]

    if interval in presets:
        return "@" + interval

    # the interval should be a CRON expression
    return interval
