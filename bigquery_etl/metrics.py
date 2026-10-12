"""Metric-hub integration."""

from typing import Dict, List, Optional, Union

import attr
from metric_config_parser.config import ConfigCollection

JETSTREAM_CONFIGS_REPO = "https://github.com/mozilla/metric-hub/tree/main/jetstream"


class _MetricHubConfigLoader:
    """Loads and caches metric-hub config collections so callers share one clone."""

    _metric_hub_configs: Optional[ConfigCollection] = None
    _experiment_configs: Optional[ConfigCollection] = None

    def metric_hub_configs(self) -> ConfigCollection:
        """Return the metric-hub root config collection."""
        if self._metric_hub_configs is None:
            self._metric_hub_configs = ConfigCollection.from_github_repo()
        return self._metric_hub_configs

    def experiment_configs(self) -> ConfigCollection:
        """Return metric-hub root configs merged with jetstream experiment configs.

        Kept separate from `metric_hub_configs` because metric-hub/jetstream
        definitions shadow the root definitions on merge (other wins), which
        would change the SQL `MetricHub.calculate`/`.data_source` generate.
        """
        if self._experiment_configs is None:
            self._experiment_configs = ConfigCollection.from_github_repos(
                [ConfigCollection.repo_url, JETSTREAM_CONFIGS_REPO]
            )
        return self._experiment_configs


MetricHubConfigLoader = _MetricHubConfigLoader()


@attr.s(auto_attribs=True, slots=True)
class MetricHub:
    """Metric-hub integration for generating SQL from referenced metrics."""

    @property
    def config_collection(self):
        """Config collection instance."""
        return MetricHubConfigLoader.metric_hub_configs()

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
