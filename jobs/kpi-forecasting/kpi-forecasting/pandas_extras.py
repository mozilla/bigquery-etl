import numpy as np
import pandas as pd


def percentile(p: int = 50, name_format: str = "p{:02.0f}"):
    """A method to calculate percentiles along dataframe axes via the `pandas.agg` method."""

    def f(x):
        return x.quantile(p / 100)

    f.__name__ = name_format.format(p)
    return f


def aggregate_to_period(
    df: pd.DataFrame,
    period: str,
    aggregation: callable = np.sum,
    date_col: str = "submission_date",
) -> pd.DataFrame:
    """Floor dates to the correct period and aggregate."""
    if period.lower() not in ["day", "month", "year"]:
        raise ValueError(
            f"Don't know how to floor dates by {period}. Please use 'day', 'month', or 'year'."
        )

    x = df.copy(deep=True)
    x[date_col] = pd.to_datetime(x[date_col]).dt.to_period(period[0]).dt.to_timestamp()
    return x.groupby(date_col).agg(aggregation).reset_index()
