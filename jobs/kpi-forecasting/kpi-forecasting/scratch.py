from datetime import datetime

import pandas as pd
import yaml

from Utils.PosteriorSampling import get_aggregated_posteriors
from Utils.FitForecast import run_forecast

filepath = "~/map-projects/kpi_accounting_21/python/mobile_dau.csv"

df = pd.read_csv(filepath, header=0)

with open("./yaml/mobile.yaml", "r") as f:
    config = yaml.safe_load(f)

df = df.rename({"submission_date": "ds", "cdou": "y"}, axis=1)

df = df[["ds", "y"]]


def make_datetime(input):
    return datetime.strptime(input, "%Y-%m-%d").date()


df["ds"] = df["ds"].apply(make_datetime)
df["ds_month"] = df["ds"].apply(lambda x: x.month)

samples = run_forecast(df, config)

# samples["ds"] = samples["ds"].astype(str).apply(make_datetime)
# samples["ds_month"] = samples["ds"].apply(lambda x: x.month)

testing_df = get_aggregated_posteriors(
    observed_data=df,
    posterior_samples=samples,
    aggregation_unit="ds_month",
    final_sample_date=samples["ds"].max(),
    actuals_end_date=df["ds"].max(),
    target="mobile",
)

print(testing_df)
