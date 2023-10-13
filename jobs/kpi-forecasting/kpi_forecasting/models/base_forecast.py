import json
import numpy as np
import pandas as pd

from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames as bq_types
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from kpi_forecasting import pandas_extras as pdx
from kpi_forecasting.metric_hub import MetricHub
from pandas.api import types as pd_types
from typing import Dict, List, Tuple


@dataclass
class BaseForecast:
    """
    A base class for fitting, forecasting, and summarizing forecasts. This class
    should not be invoked directly; it should be inherited by a child class. The
    child class needs to implement `_fit` and `_forecast` methods in order to work.

    Args:
        model_type (str): The name of the forecasting model that's being used.
        parameters (Dict): Parameters that should be passed to the forecasting model.
        use_holidays (bool): Whether or not the forecasting model should use holidays.
            The base model does not apply holiday logic; that logic needs to be built
            in the child class.
        start_date (str): A 'YYYY-MM-DD' formatted-string that specifies the first
            date that should be forecsted.
        end_date (str): A 'YYYY-MM-DD' formatted-string that specifies the last
            date the metric should be queried.
        metric_hub (MetricHub): A MetricHub object that provides details about the
            metric to be forecasted.
        number_of_simulations (int): The number of simulated timeseries that the forecast
            should generate. Since many forecast models are probablistic, this enables the
            measurement of variation across a range of possible outcomes.
    """

    model_type: str
    parameters: Dict
    use_holidays: bool
    start_date: str
    end_date: str
    metric_hub: MetricHub
    number_of_simulations: int = 1000

    def __post_init__(self) -> None:
        # fetch observed observed data
        self.collected_at = datetime.utcnow()
        self.observed_df = self.metric_hub.fetch()

        # use default start/end dates if the user doesn't specify them
        self.start_date = pd.to_datetime(self.start_date or self._default_start_date)
        self.end_date = pd.to_datetime(self.end_date or self._default_end_date)
        self.dates_to_predict = pd.DataFrame(
            {"submission_date": pd.date_range(self.start_date, self.end_date).date}
        )

        # initialize unset attributes
        self.model = None
        self.forecast_df = None
        self.summary_df = None

        # metadata
        self.metadata_params = json.dumps(
            {
                "model_type": self.model_type.lower(),
                "model_params": self.parameters.toDict(),
                "use_holidays": self.use_holidays,
            }
        )

    def _fit(self) -> None:
        """
        Fit a forecasting model using `self.observed_df` that was generated using
        Metric Hub data. This method should update `self.model`.
        """
        raise NotImplementedError

    def _predict(self) -> pd.DataFrame:
        """
        Forecast using `self.model`. This method should return a dataframe that will
        be validated by `_validate_forecast_df`.
        """
        raise NotImplementedError

    def _predict_legacy(self) -> pd.DataFrame:
        """
        Forecast using `self.model`, adhering to the legacy data format.
        """
        # TODO: This method should be removed once the forecasting data model is updated:
        # https://mozilla-hub.atlassian.net/browse/DS-2676
        raise NotImplementedError

    @property
    def _default_start_date(self) -> str:
        """The first day after the last date in the observed dataset."""
        return self.observed_df["submission_date"].max() + timedelta(days=1)

    @property
    def _default_end_date(self) -> str:
        """78 weeks (18 months) ahead of the current UTC date."""
        return (datetime.utcnow() + timedelta(weeks=78)).date()

    def _set_seed(self) -> None:
        """Set random seed to ensure that fits and predictions are reproducible."""
        np.random.seed(42)

    def _validate_forecast_df(self) -> None:
        """Validate that `self.forecast_df` has been generated correctly."""
        df = self.forecast_df
        columns = df.columns
        expected_shape = (len(self.dates_to_predict), 1 + self.number_of_simulations)
        numeric_columns = df.drop(columns="submission_date").columns

        if "submission_date" not in columns:
            raise ValueError("forecast_df must contain a 'submission_date' column.")

        if df.shape != expected_shape:
            raise ValueError(
                f"Expected forecast_df to have shape {expected_shape}, but it has shape {df.shape}."
            )

        if not df["submission_date"].equals(self.dates_to_predict["submission_date"]):
            raise ValueError(
                "forecast_df['submission_date'] does not match dates_to_predict['submission_date']."
            )

        for i in numeric_columns:
            if not pd_types.is_numeric_dtype(self.forecast_df[i]):
                raise ValueError(
                    "All forecast_df columns except 'submission_date' must be numeric,"
                    f" but column {i} has type {df[i].dtypes}."
                )

    def _summarize(
        self,
        period: str,
        numpy_aggregations: List[str],
        percentiles: List[int],
    ) -> pd.DataFrame:
        """
        Calculate summary metrics for `self.forecast_df` over a given period, and
        add metadata.
        """
        # build a list of all functions that we'll summarize the data by
        aggregations = [getattr(np, i) for i in numpy_aggregations]
        aggregations.extend([pdx.percentile(i) for i in percentiles])

        # aggregate metric to the correct date period (day, month, year)
        observed_summarized = pdx.aggregate_to_period(self.observed_df, period)
        forecast_agg = pdx.aggregate_to_period(self.forecast_df, period)

        # find periods of overlap between observed and forecasted data
        overlap = forecast_agg.merge(
            observed_summarized,
            on="submission_date",
            how="left",
        ).fillna(0)

        forecast_summarized = (
            forecast_agg.set_index("submission_date")
            # Add observed data samples to any overlapping forecasted period. This
            # ensures that any forecast made partway through a period accounts for
            # previously observed data within the period. For example, when a monthly
            # forecast is generated in the middle of the month.
            .add(overlap[["value"]].values)
            # calculate summary values, aggregating by submission_date,
            .agg(aggregations, axis=1).reset_index()
            # "melt" the df from wide-format to long-format.
            .melt(id_vars="submission_date", var_name="measure")
        )

        # add datasource-specific metadata columns
        forecast_summarized["source"] = "forecast"
        observed_summarized["source"] = "historical"
        observed_summarized["measure"] = "observed"

        # create a single dataframe that contains observed and forecasted data
        df = pd.concat([observed_summarized, forecast_summarized])

        # add summary metadata columns
        df["aggregation_period"] = period.lower()

        # reorder columns to make interpretation easier
        df = df[["submission_date", "aggregation_period", "source", "measure", "value"]]

        # add Metric Hub metadata columns
        df["metric_alias"] = self.metric_hub.alias.lower()
        df["metric_hub_app_name"] = self.metric_hub.app_name.lower()
        df["metric_hub_slug"] = self.metric_hub.slug.lower()
        df["metric_start_date"] = pd.to_datetime(self.metric_hub.min_date)
        df["metric_end_date"] = pd.to_datetime(self.metric_hub.max_date)
        df["metric_collected_at"] = self.collected_at

        # add forecast model metadata columns
        df["forecast_start_date"] = self.start_date
        df["forecast_end_date"] = self.end_date
        df["forecast_trained_at"] = self.trained_at
        df["forecast_predicted_at"] = self.predicted_at
        df["forecast_parameters"] = self.metadata_params

        return df

    def _summarize_legacy(self) -> pd.DataFrame:
        """
        Converts a `self.summary_df` to the legacy format used in
        `moz-fx-data-shared-prod.telemetry_derived.kpi_automated_forecast_confidences_v1`
        """
        # TODO: This method should be removed once the forecasting data model is updated:
        # https://mozilla-hub.atlassian.net/browse/DS-2676

        df = self.summary_df.copy(deep=True)

        # rename columns to legacy values
        df.rename(
            columns={
                "forecast_end_date": "asofdate",
                "submission_date": "date",
                "metric_alias": "target",
                "aggregation_period": "unit",
            },
            inplace=True,
        )
        df["forecast_date"] = df["forecast_predicted_at"].dt.date
        df["type"] = df["source"].replace("historical", "actual")
        df = df.replace(
            {
                "measure": {
                    "observed": "value",
                    "p05": "yhat_p5",
                    "p10": "yhat_p10",
                    "p20": "yhat_p20",
                    "p30": "yhat_p30",
                    "p40": "yhat_p40",
                    "p50": "yhat_p50",
                    "p60": "yhat_p60",
                    "p70": "yhat_p70",
                    "p80": "yhat_p80",
                    "p90": "yhat_p90",
                    "p95": "yhat_p95",
                },
                "target": {
                    "desktop_dau": "desktop",
                    "mobile_dau": "mobile",
                },
            }
        )

        # pivot the df from "long" to "wide" format
        index_columns = [
            "asofdate",
            "date",
            "target",
            "unit",
            "forecast_parameters",
            "forecast_date",
        ]
        df = (
            df[index_columns + ["measure", "value"]]
            .pivot(
                index=index_columns,
                columns="measure",
                values="value",
            )
            .reset_index()
        )

        # pivot sets the "name" attribute of the columns for some reason. It's
        # None by default, so we just reset that here.
        df.columns.name = None

        # When there's an overlap in the observed and forecasted period -- for
        # example, when a monthly forecast is generated mid-month -- the legacy
        # format only records the forecasted value, not the observed value. To
        # account for this, we'll just find the max of the "mean" (forecasted) and
        # "value" (observed) data. In all non-overlapping observed periods, the
        # forecasted value will be NULL. In all non-overlapping forecasted periods,
        # the observed value will be NULL. In overlapping periods, the forecasted
        # value will always be larger because it is the sum of the observed and forecasted
        # values. Below is a query that demonstrates the legacy behavior:
        #
        # SELECT *
        #   FROM `moz-fx-data-shared-prod.telemetry_derived.kpi_automated_forecast_confidences_v1`
        #  WHERE asofdate = "2023-12-31"
        #    AND target = "mobile"
        #    AND unit = "month"
        #    AND forecast_date = "2022-06-04"
        #    AND date BETWEEN "2022-05-01" AND "2022-06-01"
        #  ORDER BY date
        df["value"] = df[["mean", "value"]].max(axis=1)
        df.drop(columns=["mean"], inplace=True)

        # non-numeric columns are represented in the legacy bq schema as strings
        string_cols = [
            "asofdate",
            "date",
            "target",
            "unit",
            "forecast_parameters",
            "forecast_date",
        ]
        df[string_cols] = df[string_cols].astype(str)

        return df

    def fit(self) -> None:
        """Fit a model using historic metric data provided by `metric_hub`."""
        print(f"Fitting {self.model_type} model.", flush=True)
        self._set_seed()
        self.trained_at = datetime.utcnow()
        self._fit()

    def predict(self) -> None:
        """Generate a forecast from `start_date` to `end_date`."""
        print(f"Forecasting from {self.start_date} to {self.end_date}.", flush=True)
        self._set_seed()
        self.predicted_at = datetime.utcnow()
        self.forecast_df = self._predict()
        self._validate_forecast_df()

        # TODO: This line should be removed once the forecasting data model is updated:
        # https://mozilla-hub.atlassian.net/browse/DS-2676
        self.forecast_df_legacy = self._predict_legacy()

    def summarize(
        self,
        periods: List[str] = ["day", "month"],
        numpy_aggregations: List[str] = ["mean"],
        percentiles: List[int] = [10, 50, 90],
    ) -> None:
        """
        Calculate summary metrics for `self.forecast_df` and add metadata.
        The dataframe returned here will be reported in Big Query when
        `write_results` is called.

        Args:
            periods (List[str]): A list of the time periods that the data should be aggregated and
                summarized by. For example ["day", "month"]
            numpy_aggregations (List[str]): A list of numpy methods (represented as strings) that can
                be applied to summarize numeric values in a numpy dataframe. For example, ["mean"].
            percentiles (List[int]): A list of integers representing the percentiles that should be reported
                in the summary. For example [50] would calculate the 50th percentile (i.e. the median).
        """
        self.summary_df = pd.concat(
            [self._summarize(i, numpy_aggregations, percentiles) for i in periods]
        )

        # TODO: remove this once the forecasting data model is updated:
        # https://mozilla-hub.atlassian.net/browse/DS-2676
        self.summary_df_legacy = self._summarize_legacy()

    def write_results(
        self,
        project: str,
        dataset: str,
        table: str,
        project_legacy: str,
        dataset_legacy: str,
        write_disposition: str = "WRITE_APPEND",
        forecast_table_legacy: str = "kpi_automated_forecast_v1",
        confidences_table_legacy: str = "kpi_automated_forecast_confidences_v1",
    ) -> None:
        """
        Write `self.summary_df` to Big Query.

        Args:
            project (str): The Big Query project that the data should be written to.
            dataset (str): The Big Query dataset that the data should be written to.
            table (str): The Big Query table that the data should be written to.
            write_disposition (str): In the event that the destination table exists,
                should the table be overwritten ("WRITE_TRUNCATE") or appended to
                ("WRITE_APPEND")?
        """
        print(f"Writing results to `{project}.{dataset}.{table}`.", flush=True)
        client = bigquery.Client(project=project)
        schema = [
            bigquery.SchemaField("submission_date", bq_types.DATE),
            bigquery.SchemaField("aggregation_period", bq_types.STRING),
            bigquery.SchemaField("source", bq_types.STRING),
            bigquery.SchemaField("measure", bq_types.STRING),
            bigquery.SchemaField("value", bq_types.FLOAT),
            bigquery.SchemaField("metric_alias", bq_types.STRING),
            bigquery.SchemaField("metric_hub_app_name", bq_types.STRING),
            bigquery.SchemaField("metric_hub_slug", bq_types.STRING),
            bigquery.SchemaField("metric_start_date", bq_types.DATE),
            bigquery.SchemaField("metric_end_date", bq_types.DATE),
            bigquery.SchemaField("metric_collected_at", bq_types.TIMESTAMP),
            bigquery.SchemaField("forecast_start_date", bq_types.DATE),
            bigquery.SchemaField("forecast_end_date", bq_types.DATE),
            bigquery.SchemaField("forecast_trained_at", bq_types.TIMESTAMP),
            bigquery.SchemaField("forecast_predicted_at", bq_types.TIMESTAMP),
            bigquery.SchemaField("forecast_parameters", bq_types.STRING),
        ]
        job = client.load_table_from_dataframe(
            dataframe=self.summary_df,
            destination=f"{project}.{dataset}.{table}",
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                autodetect=False,
                write_disposition=write_disposition,
            ),
        )
        # Wait for the job to complete.
        job.result()

        # TODO: remove the below jobs once the forecasting data model is updated:
        # https://mozilla-hub.atlassian.net/browse/DS-2676

        job = client.load_table_from_dataframe(
            dataframe=self.forecast_df_legacy,
            destination=f"{project_legacy}.{dataset_legacy}.{forecast_table_legacy}",
            job_config=bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                schema=[
                    bigquery.SchemaField("ds", bq_types.TIMESTAMP),
                    bigquery.SchemaField("forecast_date", bq_types.STRING),
                    bigquery.SchemaField("forecast_parameters", bq_types.STRING),
                ],
            ),
        )
        job.result()

        job = client.load_table_from_dataframe(
            dataframe=self.summary_df_legacy,
            destination=f"{project_legacy}.{dataset_legacy}.{confidences_table_legacy}",
            job_config=bigquery.LoadJobConfig(
                write_disposition=write_disposition,
                schema=[
                    bigquery.SchemaField("asofdate", bq_types.STRING),
                    bigquery.SchemaField("date", bq_types.STRING),
                ],
            ),
        )
        job.result()
