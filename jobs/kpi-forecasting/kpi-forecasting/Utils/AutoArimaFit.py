from datetime import datetime
from datetime import date
import typing

import pandas as pd

from statsforecast.core import StatsForecast
from statsforecast.models import AutoARIMA


def run_forecast_arima(dataset: pd.DataFrame, config: dict) -> pd.DataFrame:

    fit_parameters = config[
        "forecast_parameters"
    ].copy()  # you must force a copy here or it assigns a reference to
    # the dictionary

    dataset["unique_id"] = 0

    models = [AutoARIMA(season_length=365, approximation=True)]

    periods = len(
        pd.date_range(start=date.today(), end=config["stop_date"], freq="d").to_list()
    )

    fcst = StatsForecast(df=dataset, models=models, freq="D", n_jobs=-1)

    # this method reflects the percentiles around the mean, so do half the range only
    percentiles = [60, 70, 80, 90, 95]
    forecast = fcst.forecast(periods, level=percentiles)

    percentile_columns = [
        "AutoARIMA-lo-95",
        "AutoARIMA-lo-90",
        "AutoARIMA-lo-80",
        "AutoARIMA-lo-70",
        "AutoARIMA-lo-60",
        "AutoARIMA-hi-60",
        "AutoARIMA-hi-70",
        "AutoARIMA-hi-80",
        "AutoARIMA-hi-90",
        "AutoARIMA-hi-95",
    ]

    column_name_correction_dict = {}
    for column in percentile_columns:
        if "-" in column:
            bits = column.split("-")

            if bits[1] == "lo":
                percentile = 100 - int(bits[2])
            else:
                percentile = bits[2]

            column_name_correction_dict[column] = f"yhat_p{percentile}"

    column_name_correction_dict["ds"] = "date"

    percentile_columns.append("ds")

    confidences = forecast[percentile_columns]  # pd.DataFrame

    confidences["asofdate"] = forecast["ds"].max()
    confidences["forecast_date"] = date.today()
    confidences["yhat_p50"] = forecast["AutoARIMA"]
    confidences["target"] = config["target"]
    confidences["unit"] = "day"
    confidences["forecast"] = forecast["AutoARIMA"]

    confidences = confidences.rename(columns=column_name_correction_dict)

    confidences = confidences[
        [
            "asofdate",
            "date",
            "target",
            "unit",
            "forecast",
            "yhat_p5",
            "yhat_p10",
            "yhat_p20",
            "yhat_p30",
            "yhat_p40",
            "yhat_p50",
            "yhat_p60",
            "yhat_p70",
            "yhat_p80",
            "yhat_p90",
            "yhat_p95",
        ]
    ]

    confidences.reset_index(inplace=True, drop=True)

    return confidences


def remaining_days(max_day, end_date) -> int:
    if type(max_day) == str:
        parts = [int(part) for part in max_day.split("-")]
        max_day = datetime(year=parts[0], month=parts[1], day=parts[2]).date()

    if type(end_date) == str:
        parts = [int(part) for part in end_date.split("-")]
        end_date = datetime(year=parts[0], month=parts[1], day=parts[2]).date()

    return (end_date - max_day).days
