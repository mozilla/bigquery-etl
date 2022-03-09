from datetime import datetime

import pandas as pd

import prophet

import holidays


def run_forecast(dataset: pd.DataFrame, config: dict) -> pd.DataFrame:
    holiday_df = pd.DataFrame.from_dict(
        holidays.US(years=[2017, 2018, 2019, 2020, 2021]).items()
    )  # type: pd.DataFrame
    holiday_df.rename({0: "ds", 1: "holiday"}, inplace=True, axis=1)

    target = config["target"]

    fit_parameters = config[
        "forecast_parameters"
    ].copy()  # you must force a copy here or it assigns a reference to
    # the dictionary
    fit_parameters["holidays"] = holiday_df
    fit_parameters["growth"] = "flat"

    model = prophet.Prophet(**fit_parameters)

    if target == "desktop":
        model.add_regressor(name="regressor_00")

    elif target == "mobile":
        step_change_date = datetime.strptime("2021-1-24", "%Y-%m-%d").date()
        dataset["regressor_00"] = dataset.apply(
            lambda x: 0 if x["ds"] <= step_change_date else 1, axis=1
        )  # because of a step change in the data mobile data needs this thumb on the scale

        model.add_regressor(name="regressor_00")

    fit_model = model.fit(dataset)

    predictions = fit_model.predict()  # type: pd.DataFrame

    periods = remaining_days(dataset["ds"].max())

    future = fit_model.make_future_dataframe(periods=periods)  # type: pd.DataFrame

    future["regressor_00"] = 1
    future_values = fit_model.predict(future)

    return future_values


def remaining_days(max_day) -> int:
    if type(max_day) == str:
        parts = [int(part) for part in max_day.split("-")]
        max_day = datetime(year=parts[0], month=parts[1], day=parts[2])

    end_of_year = datetime(year=max_day.year, month=12, day=31).date()

    return (end_of_year - max_day).days
