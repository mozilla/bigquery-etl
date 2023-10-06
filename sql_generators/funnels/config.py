import attr
from typing import Optional, Dict, List
from enum import Enum
from bigquery_etl.metrics import MetricHub


@attr.s(auto_attribs=True)
class Dimension:
    data_source: str
    select_expression: str


class Aggregation(Enum):
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    COUNT_DISTINCT = "count distinct"
    MEAN = "mean"

    def sql(self, column: str) -> str:
        """Returns the SQL snippet."""

        if self.value == "min":
            return f"MIN({column})"
        elif self.value == "max":
            return f"MAX({column})"
        elif self.value == "count":
            return f"COUNT({column})"
        elif self.value == "count distinct":
            return f"COUNT(DISTINCT {column})"
        elif self.value == "mean":
            return f"AVG({column})"
        else:
            raise ValueError(f"No SQL implemented for {self.value}")


@attr.s(auto_attribs=True)
class Funnel:
    steps: List[str]
    dimensions: Optional[List[str]] = attr.ib(None)


@attr.s(auto_attribs=True)
class Step:
    data_source: str
    aggregation: Aggregation
    select_expression: str
    depends_on_previous_step: bool = False
    friendly_name: Optional[str] = attr.ib(None)
    description: Optional[str] = attr.ib(None)
    where_expression: Optional[str] = attr.ib(None)
    join_key: Optional[str] = attr.ib(None)


@attr.s(auto_attribs=True)
class DataSource:
    from_expression: str
    submission_date_column: str = attr.ib("submission_date")
    client_id_column: str = attr.ib("client_id")


@attr.s(auto_attribs=True)
class FunnelConfig:
    funnels: Dict[str, Funnel]
    steps: Dict[str, Step]
    dimensions: Dict[str, Dimension] = attr.ib({})
    data_sources: Dict[str, DataSource] = attr.ib({})
    destination_dataset: str = attr.ib("telemetry_derived")
    version: str = attr.ib("1")
    platform: Optional[str] = attr.ib(None)

    def __attrs_post_init__(self):
        # check if metric-hub data source was referenced
        metric_hub = MetricHub()

        for step_name, step in self.steps.items():
            if step.data_source not in self.data_sources:
                if not self.platform:
                    raise ValueError(
                        f"Undefined data source {step.data_source} for step {step_name}. "
                        + "If you are referencing a metric-hub data source, please specify the platform."
                    )
                else:
                    data_source_sql = metric_hub.data_source(
                        data_source=step.data_source, platform=self.platform
                    )
                    self.data_sources[step.data_source] = DataSource(
                        from_expression=data_source_sql
                    )

        for _, dimension in self.dimensions.items():
            if dimension.data_source not in self.data_sources:
                if not self.platform:
                    raise ValueError(
                        f"Undefined data source {step.data_source} for step {step_name}. "
                        + "If you are referencing a metric-hub data source, please specify the platform."
                    )
                else:
                    data_source_sql = metric_hub.data_source(
                        data_source=dimension.data_source, platform=self.platform
                    )
                    self.data_sources[step.data_source] = DataSource(
                        from_expression=data_source_sql
                    )

        # todo: allow referencing dimensions from metric-hub
