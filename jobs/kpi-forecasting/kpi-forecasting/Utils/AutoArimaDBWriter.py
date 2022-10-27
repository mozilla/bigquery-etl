from datetime import date
import json
from typing import Optional

import pandas as pd

from google.cloud import bigquery, client


def write_arima_results_to_bigquery(
    arima_results: pd.DataFrame, config: dict, client: Optional[client.Client] = None
):
    project = config["write_project"]

    if client is None:
        bq_client = bigquery.Client(project=project)
    else:
        bq_client = client

    today = str(date.today())

    config["forecast_parameters"]["holidays"] = config["holidays"]
    forecast_parameters = json.dumps(config["forecast_parameters"])

    arima_results["target"] = config["target"]
    arima_results["forecast_date"] = today
    arima_results["forecast_parameters"] = str(forecast_parameters)

    output_table = config["output_table"]

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("forecast_date", "STRING"),
            bigquery.SchemaField("forecast_parameters", "STRING"),
        ]
    )

    predictions = arima_results.assign(metric=config["forecast_variable"])

    database_job = bq_client.load_table_from_dataframe(
        predictions, output_table, job_config=job_config
    )

    result = database_job.result()

    return result
