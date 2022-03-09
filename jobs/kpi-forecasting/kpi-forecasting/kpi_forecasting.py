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
from Utils.DBWriter import write_to_bigquery


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--config", type=str, help="Path to configuration yaml file"
    )
    return parser.parse_args()


def main() -> None:
    args = get_args()
    with open(args.config, "r") as config_stream:
        config = yaml.safe_load(config_stream)

    dataset, _ = fetch_data(config)

    predictions = run_forecast(dataset, config)

    write_to_bigquery(
        predictions, config, None
    )  # None here is potentially a bigquery_client IFF your input and output datasets share the same project


if __name__ == "__main__":
    main()
