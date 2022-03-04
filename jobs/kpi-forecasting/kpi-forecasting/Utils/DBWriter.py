from datetime import date
import json

import pandas as pd

from google.cloud import bigquery, client


def write_to_bigquery(predictions: pd.DataFrame, config: dict, client: client) -> None:
    project = "moz-fx-data-bq-data-science"
    bq_client = bigquery.Client(project=project)

    today = str(date.today())
    forecast_parameters = json.dumps(config["forecast_parameters"])

    predictions["target"] = config["target"]
    predictions["forecast_date"] = today
    predictions["forecast_parameters"] = str(forecast_parameters)

    output_table = "pmcmanis.automation_experiment"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("forecast_date", "STRING"),
            bigquery.SchemaField("forecast_parameters", "STRING"),
        ]
    )

    predictions = predictions[
        [
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
    ]

    database_job = bq_client.load_table_from_dataframe(
        predictions, output_table, job_config=job_config
    )

    database_job.result()
