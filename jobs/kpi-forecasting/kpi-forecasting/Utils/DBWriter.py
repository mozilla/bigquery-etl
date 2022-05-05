from datetime import date
import json

import pandas as pd

from google.cloud import bigquery, client


def write_predictions_to_bigquery(
    predictions: pd.DataFrame, config: dict, client: client = None
) -> None:
    project = config["write_project"]
    if client is None:
        bq_client = bigquery.Client(project=project)
    else:
        bq_client = client

    today = str(date.today())
    config["forecast_parameters"]["holidays"] = config["holidays"]
    forecast_parameters = json.dumps(config["forecast_parameters"])

    predictions["target"] = config["target"]
    predictions["forecast_date"] = today
    predictions["forecast_parameters"] = str(forecast_parameters)

    output_table = config["output_table"]

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("forecast_date", "STRING"),
            bigquery.SchemaField("forecast_parameters", "STRING"),
        ]
    )

    # schema compliance; Prophet does not export consistent dataframes
    holiday_columns = ["holidays", "holidays_lower", "holidays_upper"]

    for column in holiday_columns:
        if column not in predictions.columns:
            predictions[column] = 0.0

    regressor_columns = [
        "extra_regressors_additive",
        "extra_regressors_additive_lower",
        "extra_regressors_additive_upper",
        "regressor_00",
        "regressor_00_lower",
        "regressor_00_upper",
    ]

    for column in regressor_columns:
        if column not in predictions.columns:
            predictions[column] = 0.0

    columns = [
        "ds",
        "trend",
        "yhat_lower",
        "yhat_upper",
        "trend_lower",
        "trend_upper",
        "additive_terms",
        "additive_terms_lower",
        "additive_terms_upper",
        "extra_regressors_additive",
        "extra_regressors_additive_lower",
        "extra_regressors_additive_upper",
        "holidays",
        "holidays_lower",
        "holidays_upper",
        "regressor_00",
        "regressor_00_lower",
        "regressor_00_upper",
        "weekly",
        "weekly_lower",
        "weekly_upper",
        "yearly",
        "yearly_lower",
        "yearly_upper",
        "multiplicative_terms",
        "multiplicative_terms_lower",
        "multiplicative_terms_upper",
        "yhat",
        "target",
        "forecast_date",
        "forecast_parameters",
    ]

    predictions = predictions[columns]

    predictions = predictions.assign(metric=config["forecast_variable"])

    database_job = bq_client.load_table_from_dataframe(
        predictions, output_table, job_config=job_config
    )

    result = database_job.result()

    return result


def write_confidence_intervals_to_bigquery(
    confidences: pd.DataFrame, config: dict, client: client = None
) -> None:
    project = config["write_project"]
    if client is None:
        bq_client = bigquery.Client(project=project)
    else:
        bq_client = client

    today = str(date.today())

    confidences["target"] = config["target"]
    confidences["forecast_parameters"] = str(json.dumps(config["forecast_parameters"]))
    confidences["forecast_date"] = today

    write_table = config["confidences_table"]

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("asofdate", "STRING"),
            bigquery.SchemaField("date", "STRING"),
        ]
    )

    database_job = bq_client.load_table_from_dataframe(
        confidences, write_table, job_config=job_config
    )

    result = database_job.result()

    return result
