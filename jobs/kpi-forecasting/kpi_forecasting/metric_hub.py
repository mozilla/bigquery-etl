import pandas as pd

from dataclasses import dataclass
from datetime import datetime
from google.cloud import bigquery
from mozanalysis.config import ConfigLoader
from textwrap import dedent
from typing import Dict

from kpi_forecasting.utils import parse_end_date


@dataclass
class MetricHub:
    """
    Programatically get Metric Hub metrics from Big Query.
    See https://mozilla.github.io/metric-hub/metrics/ for a list of metrics.

    Args:
        app_name (str): The Metric Hub app name for the metric.
        slug (str): The Metric Hub slug for the metric.
        start_date (str): A 'YYYY-MM-DD' formatted-string that specifies the first
            date the metric should be queried.
        segments (Dict): A dictionary of segments to use to group metric values.
            The keys of the dictionary are aliases for the segment, and the
            value is a SQL snippet that defines the segment.
        where (str): A string specifying a condition to inject into a SQL WHERE clause,
            to filter the data source.
        end_date (str): A 'YYYY-MM-DD' formatted-string that specifies the last
            date the metric should be queried.
        alias (str): An alias for the metric. For example, 'DAU' instead of
            'daily_active_users'.
        project (str): The Big Query project to use when establishing a connection
            to the Big Query client.
    """

    app_name: str
    slug: str
    start_date: str
    segments: Dict[str, str] = None
    where: str = None
    end_date: str = None
    alias: str = None
    project: str = "mozdata"

    def __post_init__(self) -> None:
        self.start_date = pd.to_datetime(self.start_date).date()
        self.end_date = pd.to_datetime(parse_end_date(self.end_date)).date()

        # Set useful attributes based on the Metric Hub definition
        metric = ConfigLoader.get_metric(
            metric_slug=self.slug,
            app_name=self.app_name,
        )
        self.metric = metric
        self.alias = self.alias or metric.name
        self.submission_date_column = metric.data_source.submission_date_column

        # Modify the metric source table string so that it formats nicely in the query.
        self.from_expression = self.metric.data_source._from_expr.replace(
            "\n", "\n" + " " * 19
        )

        # Add query snippets for segments
        self.segment_select_query = ""
        self.segment_groupby_query = ""

        if self.segments:
            segment_select_query = []
            for alias, sql in self.segments.items():
                segment_select_query.append(f"  {sql} AS {alias},")
            self.segment_select_query = "," + "\n              ".join(
                segment_select_query
            )
            self.segment_groupby_query = "," + "\n             ,".join(
                self.segments.keys()
            )

        self.where = f"AND {self.where}" if self.where else ""

    def query(self) -> str:
        """Build a string to query the relevant metric values from Big Query."""
        return dedent(
            f"""
            SELECT {self.submission_date_column} AS submission_date,
                {self.metric.select_expr} AS value
                    {self.segment_select_query}
            FROM {self.from_expression}
            WHERE {self.submission_date_column} BETWEEN '{self.start_date}' AND '{self.end_date}'
                {self.where}
            GROUP BY {self.submission_date_column}
                    {self.segment_groupby_query}
        """
        )

    def fetch(self) -> pd.DataFrame:
        """Fetch the relevant metric values from Big Query."""
        print(
            f"\nQuerying for '{self.app_name}.{self.slug}' aliased as '{self.alias}':"
            f"\n{self.query()}"
        )
        df = bigquery.Client(project=self.project).query(self.query()).to_dataframe()

        # ensure submission_date has type 'date'
        df[self.submission_date_column] = pd.to_datetime(
            df[self.submission_date_column]
        ).dt.date

        # Track the min and max dates in the data, which may differ from the
        # start/end dates
        self.min_date = str(df[self.submission_date_column].min())
        self.max_date = str(df[self.submission_date_column].max())

        return df
