from datetime import datetime

from dateutil.relativedelta import relativedelta

import numpy as np
import pandas as pd


def fill_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    df["ds_day"] = df["ds"]
    df["ds_month"] = df["ds"].values.astype("datetime64[M]")
    df["ds_year"] = df["ds"].values.astype("datetime64[Y]")
    return df


def get_confidence_intervals(
    observed_data: pd.DataFrame,
    uncertainty_samples: pd.DataFrame,
    aggregation_unit_of_time,
    asofdate,
    final_observed_sample_date,
    target,
):
    asofdate = pd.to_datetime(asofdate)

    uncertainty_samples = fill_date_columns(uncertainty_samples)

    observed_data = fill_date_columns(observed_data)
    observed_data["ds"] = pd.to_datetime(observed_data["ds"])

    samples_df_grouped = (
        uncertainty_samples[
            uncertainty_samples["ds"] > np.datetime64(final_observed_sample_date)
        ]
        .groupby("{}".format(aggregation_unit_of_time))
        .sum()
    )

    print(samples_df_grouped.tail())
    # start the aggregated dataframe with the mean of the uncertainty samples
    uncertainty_samples_aggregated = samples_df_grouped.mean(axis=1).reset_index()

    uncertainty_samples_aggregated.rename(columns={0: "value"}, inplace=True)

    percentiles = [5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95]
    aggregation_columns = [
        "asofdate",
        "date",
        "target",
        "unit",
        "value",
    ]

    for percentile in percentiles:
        percentile_column_name = f"yhat_p{percentile}"
        uncertainty_samples_aggregated[
            percentile_column_name
        ] = samples_df_grouped.apply(
            lambda x: np.percentile(x, percentile), axis=1
        ).tolist()
        aggregation_columns.append(percentile_column_name)

    observed_aggregated = (
        observed_data.loc[
            observed_data["ds"] <= np.datetime64(final_observed_sample_date),
            ["{}".format(aggregation_unit_of_time), "y"],
        ]
        .groupby("{}".format(aggregation_unit_of_time))
        .agg({"y": np.sum})
        .reset_index()
    )
    observed_aggregated = observed_aggregated.rename(
        columns={"y": "value"}
    ).sort_values(by="{}".format(aggregation_unit_of_time))

    # check if whether there are overlap in actual and forecast at the group level
    if (
        aggregation_unit_of_time == "ds_month"
        and (pd.to_datetime(final_observed_sample_date) + relativedelta(days=1)).day
        != 1
    ) or (
        aggregation_unit_of_time == "ds_year"
        and (
            pd.to_datetime(final_observed_sample_date) + relativedelta(days=1)
        ).dayofyear
        != 1
    ):
        uncertainty_samples_aggregated.at[0, 1:] = (
            uncertainty_samples_aggregated.iloc[0, 1:]
            + observed_aggregated.iloc[-1].value
        )
        observed_aggregated = observed_aggregated.loc[
            observed_aggregated[aggregation_unit_of_time]
            < observed_aggregated[aggregation_unit_of_time].max()
        ]

    observed_aggregated["type"] = "actual"
    uncertainty_samples_aggregated["type"] = "forecast"
    all_aggregated = pd.merge(
        observed_aggregated,
        uncertainty_samples_aggregated,
        on=["{}".format(aggregation_unit_of_time), "value", "type"],
        how="outer",
    )
    all_aggregated["asofdate"] = asofdate
    all_aggregated["target"] = target
    all_aggregated["unit"] = aggregation_unit_of_time.replace("ds_", "")

    all_aggregated = all_aggregated.rename(
        columns={"{}".format(aggregation_unit_of_time): "date"}
    )
    all_aggregated["date"] = pd.to_datetime(all_aggregated["date"]).dt.strftime(
        "%Y-%m-%d"
    )
    all_aggregated["asofdate"] = pd.to_datetime(all_aggregated["asofdate"]).dt.strftime(
        "%Y-%m-%d"
    )

    return all_aggregated[aggregation_columns]
