"""Metric-hub integration."""

from typing import Dict, List, Optional, Union

import attr
from metric_config_parser.config import ConfigCollection


@attr.s(auto_attribs=True, slots=True)
class MetricHub:
    """Metric-hub integration for generating SQL from referenced metrics."""

    _config_collection: Optional[ConfigCollection] = None

    @property
    def config_collection(self):
        """Config collection instance."""
        self._config_collection = (
            self._config_collection or ConfigCollection.from_github_repo()
        )
        return self._config_collection

    def calculate(
        self,
        metrics: List[str],
        platform: str,
        group_by: Union[List[str], Dict[str, str]] = [],
        where: Optional[str] = None,
        group_by_client_id: bool = True,
        group_by_submission_date: bool = True,
    ) -> str:
        """Generate SQL query for specified metrics."""
        return self.config_collection.get_metrics_sql(
            metrics=metrics,
            platform=platform,
            group_by=group_by,
            where=where,
            group_by_client_id=group_by_client_id,
            group_by_submission_date=group_by_submission_date,
        )

    def data_source(
        self,
        data_source: str,
        platform: str,
        where: Optional[str] = None,
    ) -> str:
        """Generate SQL query for specified data source."""
        return self.config_collection.get_data_source_sql(
            data_source=data_source, platform=platform, where=where
        )
