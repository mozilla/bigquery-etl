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
from Utils.AutoArimaFit import run_forecast_arima
from Utils.DBWriter import (
    write_predictions_to_bigquery,
    write_confidence_intervals_to_bigquery,
)
from Utils.AutoArimaDBWriter import write_arima_results_to_bigquery
from Utils.PosteriorSampling import get_confidence_intervals


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--config", type=str, help="Path to configuration yaml file"
    )
    parser.add_argument(
        "-r",
        "--autoarima",
        action="store_true",
        help="Set this flag to use prophet instead of AutoArima",
    )
    return parser.parse_args()


def main() -> None:
    args = get_args()
    with open(args.config, "r") as config_stream:
        config = yaml.safe_load(config_stream)

    dataset = fetch_data(config)

    if not args.autoarima:
        predictions, uncertainty_samples = run_forecast(dataset, config)

        confidences = get_confidence_intervals(
            observed_data=dataset,
            uncertainty_samples=uncertainty_samples,
            aggregation_unit_of_time=config["confidences"],
            asofdate=predictions["ds"].max(),
            final_observed_sample_date=dataset["ds"].max(),
            target="desktop",
        )

        write_predictions_to_bigquery(predictions, config)

        if confidences is not None:
            write_confidence_intervals_to_bigquery(confidences, config)
    else:
        predictions = run_forecast_arima(dataset, config)

        write_arima_results_to_bigquery(predictions, config)


if __name__ == "__main__":
    main()
