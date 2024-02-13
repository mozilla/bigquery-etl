import datetime


def parse_end_date(end_date: str = None):

    dt = datetime.datetime.utcnow()
    if not end_date:
        return dt
    elif "last complete" in end_date:
        return previous_period_last_date(end_date, dt)
    return end_date


def previous_period_last_date(last_period: str, now: datetime.datetime):

    if last_period not in ["last complete month", "last complete year"]:
        raise ValueError("Unrecognized end date string.")

    if last_period == "last complete year":
        return datetime.date(now.year - 1, 12, 31)

    return datetime.date(now.year, now.month, 1) - datetime.timedelta(days=1)
