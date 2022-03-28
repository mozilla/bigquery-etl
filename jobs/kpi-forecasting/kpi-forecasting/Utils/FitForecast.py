from datetime import datetime
import typing

import pandas as pd

import prophet

import holidays


def run_forecast(
    dataset: pd.DataFrame, config: dict
) -> typing.Tuple[pd.DataFrame, pd.DataFrame]:
    target = config["target"]

    fit_parameters = config[
        "forecast_parameters"
    ].copy()  # you must force a copy here or it assigns a reference to
    # the dictionary

    if config["holidays"]:
        holiday_df = pd.DataFrame.from_dict(
            holidays.US(years=[2017, 2018, 2019, 2020, 2021]).items()
        )  # type: pd.DataFrame
        holiday_df.rename({0: "ds", 1: "holiday"}, inplace=True, axis=1)
        fit_parameters["holidays"] = holiday_df
    fit_parameters["growth"] = "flat"

    model = prophet.Prophet(**fit_parameters, mcmc_samples=0)

    if target == "mobile":
        step_change_date = datetime.strptime("2021-1-24", "%Y-%m-%d").date()
        dataset["regressor_00"] = dataset.apply(
            lambda x: 0 if x["ds"] <= step_change_date else 1, axis=1
        )  # because of a step change in the data mobile data needs this

        model.add_regressor(name="regressor_00")

    fit_model = model.fit(dataset)

    _ = fit_model.predict()  # type: pd.DataFrame

    periods = remaining_days(dataset["ds"].max(), config["stop_date"])

    future = fit_model.make_future_dataframe(periods=periods)  # type: pd.DataFrame

    if target == "mobile":
        future["regressor_00"] = 1

    future_values = fit_model.predict(future)

    future_values = future_values[future_values["ds"] > datetime.today()]

    uncertainty_samples_raw = fit_model.predictive_samples(future)

    uncertainty_samples = pd.DataFrame.from_records(uncertainty_samples_raw["yhat"])

    uncertainty_samples["ds"] = future["ds"]

    return future_values, uncertainty_samples


def remaining_days(max_day, end_date) -> int:
    if type(max_day) == str:
        parts = [int(part) for part in max_day.split("-")]
        max_day = datetime(year=parts[0], month=parts[1], day=parts[2]).date()

    if type(end_date) == str:
        parts = [int(part) for part in end_date.split("-")]
        end_date = datetime(year=parts[0], month=parts[1], day=parts[2]).date()

    return (end_date - max_day).days
