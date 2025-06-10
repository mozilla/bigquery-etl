# Bigeye - bigConfig Reference Guide

## Overview

bigConfig is a yaml-based declarative configuration-as-code approach to managing out Bigeye assets and metrics applied to them. This includes specifying which resources (in our case BQ tables) we want to monitor, what metrics / monitors we want to be used on the asset, which collection those should be bundled under, and what should happen if those metrics detect data issues. For example, send a Slack notification.

> [!IMPORTANT]
> Please keep in mind that bigConfig managed metrics cannot be modified using the Bigeye UI. The same is true the other way around, metrics defined via the UI cannot be managed via bigConfig.

---

## What happens with bigConfig after it is defined?

Bigeye provides CLI tooling that takes bigConfig files as input, compares it with the application's state, and determined a list of actions that would need to be taken in order to update the application's state to match what is defined in the bigConfig files passed in as input.

This includes actions such as creation, update, and deletion of Bigeye metrics. In our case, the [publish_bigeye_monitors](https://github.com/mozilla/telemetry-airflow/blob/main/dags/bqetl_artifact_deployment.py#L204C5-L212) task inside the `bqetl_artifact_deployment` DAG is responsible for pushing any bigConfig changes that have been merged into the main branch.


> [!CAUTION]
> When working with bigeye CLI be careful and avoid running `bigconfig apply` locally, Bigeye CLI will only work with currently specified bigConfig files. If an apply operation is executed and some configuration files are not included in the plan it would result in Bigeye metrics in those files getting `deleted`. Therefore, it is best to let the artifact_deployment DAG handle publishing Bigeye monitors.

---

## Anatomy of a bigConfig file

```yaml
type: BIGCONFIG_FILE  # required field to let bigConfig know it's a bigConfig config file
table_deployments:
  #
  - collection:
      name: 'My Collection'
      notification_channels:
        - slack: '#my-slack-channel'

    # Here we specify assets (tables) and metrics we should deloy for them
    deployments:
      # specify the asset we want to target
      - fq_table_name: project_id.project_id.dataset.table_name
        # Specify table level metrics we want to apply on the asset
        table_metrics:
          - saved_metric_id: volume
          - saved_metric_id: freshness
      - fq_table_name: project_id.project_id.dataset.table_name_2
        # Specify metrics we want to apply on individual columns
        columns:
          - column_name: submission_date
            metrics:
              - saved_metric_id: is_not_null
          - column_name: usage_profile_id
            metrics:
              - saved_metric_id: is_not_null
```

---

## Example bigConfig file

```yaml
type: BIGCONFIG_FILE
table_deployments:
  - collection:
      name: 'My Collection'
      notification_channels:
        - slack: '#data-platform-infa-wg'
    deployments:
      - fq_table_name: moz-fx-data-shared-prod.moz-fx-data-shared-prod.fenix_derived.retention_v1
        table_metrics:
          - saved_metric_id: volume
          - saved_metric_id: freshness
        columns:
          - column_name: submission_date
            metrics:
              - saved_metric_id: is_not_null
          - column_name: client_id
            metrics:
              - saved_metric_id: is_not_null
```

_Full list of all the config options available can be found in the official documentation for [table deployments](https://docs.bigeye.com/docs/bigconfig#table-deployments) and [tag deployments](https://docs.bigeye.com/docs/bigconfig#tag-definitions-optional)._

When applied, this configuration would result in Bigeye the following metrics being applied for table `moz-fx-data-shared-prod.fenix_derived.retention_v1`:

- a metric to make sure the table is updated on time
- a metric to ensure an expected number of rows is added to the table on each update
- a metric to check that both `submission_date` and `client_id` fields do not contain null values*

Those would get bundled under `My Collection` Bigeye Collection, and if any of the metrics would fail an alert would be sent to the `#data-platform-infa-wg` Slack channel.

\* _`is_not_null` is a `saved_metric_id` and is defined inside [sql/bigconfig.yml](https://github.com/mozilla/bigquery-etl/blob/main/sql/bigconfig.yml#L4C5-L19C26)_

---

### bigConfig / saved metric

A saved metric is a way of defining a Bigeye (bigConfig) metric and giving it a name. This allows us to use this metric in other bigConfig config files just by referencing the saved metric name just like we did inside `Example bigConfig file` previously.

Here's an example definition of a saved metric:

```yaml
# is_not_null
- saved_metric_id: is_not_null
  metric_type:
    predefined_metric: PERCENT_NULL
  threshold:
    type: CONSTANT
    upper_bound: 0
  schedule_frequency:
    interval_type: MINUTES
    interval_value: 0
  lookback:
    lookback_type: DATA_TIME
    lookback_window:
      interval_type: DAYS
      interval_value: 0
  rct_overrides:
  - submission_date
```

More information about bigConfig saved metrics and all options available can be found in [the official docs](https://docs.bigeye.com/docs/bigconfig#saved-metric-definitions-optional).

### A few important bigConfig metric properties

Many of the metric properies are self explanatory. There are a couple we'd like to draw attention to in particular:

- `schedule_frequency` - for many metrics we set it to `0 minutes` interval to prevent it from running on a schedule. For example, most metrics we only want to run after the dataset is updated so we have Airflow trigger the run manually
- `lookback` - allows us to specify a time window for how much data should be included in a metric run
- `rct_overrides` - allows us to overwrite which field is used to apply a lookback/date filter when running the metric

If the configuration of a saved_metric is not quite exactly as we want it we can still reference it in another bigConfig file and overwrite the values by specifying the key and the value we want to use. Here's an example:

```yaml
metrics:
- saved_metric_id: is_not_null
  lookback:
    lookback_window:
      interval_type: DAYS
      interval_value: 28
    lookback_type: DATA_TIME
  rct_overrides:
  - date
```

This would result in the `lookback` and `rct_overrides` properties to be overwritten whilst keeping the rest of the configuration.

---

## Further reading

- [What is Bigeye](https://docs.bigeye.com/docs/what-is-bigeye)
- [bigConfig](https://docs.bigeye.com/docs/bigconfig)
- [Setting up Bigeye CLI](https://docs.bigeye.com/docs/cli#installation)
- [Bigeye predefined metrics](https://docs.bigeye.com/docs/available-metrics)
