"""
Control script for KPI forecasting automation.
Most of the time, this is not the script you should be editing
Author: pmcmanis@mozilla.com
Date: Mar 2022
"""

import argparse

import yaml

from Utils.ForecastDatasets import fetch_data
from Utils.FitForecast import run_forecast


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--config", type=str, help="Path to configuration yaml file"
    )
    return parser.parse_args()


def main() -> None:
    args = get_args()
    with open(args.config, "r") as configstream:
        config = yaml.safe_load(configstream)

    dataset = fetch_data(config)

    print(dataset.head())

    import pandas as pd
    dataset = pd.read_csv("~/map-projects/kpi_accounting_21/Python/mobile_dau.csv", header=0)
    renames = {"submission_date": "ds", "cdou": "y"}

    dataset.rename(columns=renames, inplace=True)
    dataset = dataset[["ds", "y"]]

    predictions = run_forecast(dataset, config)
    print(predictions.tail())

    # write_to_bigquery(predictions)


if __name__ == "__main__":
    main()
