from datetime import datetime

from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd


def get_aggregated_posteriors(
    observed_data: pd.DataFrame,
    posterior_samples: pd.DataFrame,
    aggregation_unit: str,
    final_sample_date: datetime,
    actuals_end_date: datetime,
    target: str,
):
    assert aggregation_unit in [
        "ds_month",
        "ds_year",
    ], f"The aggregation unit of time must be one of ds_month, or ds_year, you provided {aggregation_unit}"

    observed_data = fill_date_colums(observed_data)
    posterior_samples = fill_date_colums(posterior_samples)

    final_sample_date = pd.to_datetime(final_sample_date)
    samples_df_future_dates = (
        posterior_samples.loc[posterior_samples["ds"] > np.datetime64(actuals_end_date)]
        .groupby("{}".format(aggregation_unit))
        .sum()
    )

    sample_aggregated = samples_df_future_dates.mean(axis=1).reset_index()
    sample_aggregated.rename({0: "value"}, axis=1, inplace=True)

    percentiles = [5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95]
    aggregation_columns = [
        "asofdate",
        "date",
        "target",
        "unit",
        "type",
        "value",
    ]

    for percentile in percentiles:
        percentile_column_name = f"yhat_p{percentile}"
        sample_aggregated[percentile_column_name] = samples_df_future_dates.apply(
            lambda x: np.percentile(x, percentile), axis=1
        ).tolist()
        aggregation_columns.append(percentile_column_name)

    # prepare actual data
    actual_aggregated = (
        observed_data.loc[
            observed_data["ds"] <= actuals_end_date,
            ["{}".format(aggregation_unit), "y"],
        ]
        .groupby("{}".format(aggregation_unit))
        .agg({"y": np.sum})
        .reset_index()
    )
    actual_aggregated = actual_aggregated.rename(columns={"y": "value"}).sort_values(
        by="{}".format(aggregation_unit)
    )

    # check if whether there are overlap in actual and forecast at the group level
    if (
        aggregation_unit == "ds_month"
        and (pd.to_datetime(actuals_end_date) + relativedelta(days=1)).day != 1
    ) or (
        aggregation_unit == "ds_year"
        and (pd.to_datetime(actuals_end_date) + relativedelta(days=1)).dayofyear != 1
    ):
        sample_aggregated.at[0, 1:] = (
            sample_aggregated.iloc[0, 1:] + actual_aggregated.iloc[-1].value
        )
        actual_aggregated = actual_aggregated.loc[
            actual_aggregated[aggregation_unit]
            < actual_aggregated[aggregation_unit].max()
        ]

    actual_aggregated["type"] = "actual"
    sample_aggregated["type"] = "forecast"

    aggregated_data = pd.merge(
        actual_aggregated,
        sample_aggregated,
        on=["{}".format(aggregation_unit), "value", "type"],
        how="outer",
    )
    aggregated_data["asofdate"] = final_sample_date
    aggregated_data["target"] = target
    aggregated_data["unit"] = aggregation_unit.replace("ds_", "")

    aggregated_data = aggregated_data.rename(
        columns={"{}".format(aggregation_unit): "date"}
    )
    aggregated_data["date"] = pd.to_datetime(aggregated_data["date"]).dt.strftime(
        "%Y-%m-%d"
    )
    aggregated_data["asofdate"] = pd.to_datetime(
        aggregated_data["asofdate"]
    ).dt.strftime("%Y-%m-%d")

    return aggregated_data[aggregation_columns]


def fill_date_colums(df: pd.DataFrame) -> pd.DataFrame:
    df["ds_day"] = df["ds"]
    df["ds_month"] = df["ds"].values.astype("datetime64[M]")
    df["ds_year"] = df["ds"].values.astype("datetime64[Y]")
    return df
