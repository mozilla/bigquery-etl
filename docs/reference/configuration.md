# Configuration

The behaviour of `bqetl` can be configured via the `bqetl_project.yaml` file. This file, for example, specifies the queries that should be skipped during dryrun, views that should not be published and contains various other configurations.

The general structure of `bqetl_project.yaml` is as follows:

```yaml
dry_run:
  function: https://us-central1-moz-fx-data-shared-prod.cloudfunctions.net/bigquery-etl-dryrun
  test_project: bigquery-etl-integration-test
  skip:
  - sql/moz-fx-data-shared-prod/account_ecosystem_derived/desktop_clients_daily_v1/query.sql
  - sql/**/apple_ads_external*/**/query.sql
  # - ...

views:
  skip_validation:
  - sql/moz-fx-data-test-project/test/simple_view/view.sql
  - sql/moz-fx-data-shared-prod/mlhackweek_search/events/view.sql
  - sql/moz-fx-data-shared-prod/**/client_deduplication/view.sql
  # - ...
  skip_publishing:
  - activity_stream/tile_id_types/view.sql
  - pocket/pocket_reach_mau/view.sql
  # - ...
  non_user_facing_suffixes:
  - _derived
  - _external
  # - ...

schema:
  skip_update:
  - sql/moz-fx-data-shared-prod/mozilla_vpn_derived/users_v1/schema.yaml
  # - ...
  skip_prefixes:
  - pioneer
  - rally

routines:
  skip_publishing:
  - sql/moz-fx-data-shared-prod/udf/main_summary_scalars/udf.sql

formatting:
  skip:
  - bigquery_etl/glam/templates/*.sql
  - sql/moz-fx-data-shared-prod/telemetry/fenix_events_v1/view.sql
  - stored_procedures/safe_crc32_uuid.sql
  # - ...
```

## Accessing configurations

`ConfigLoader` can be used in the bigquery_etl tooling codebase to access configuration parameters. `bqetl_project.yaml` is automatically loaded in `ConfigLoader` and parameters can be accessed via a `get()` method:

```python
from bigquery_etl.config import ConfigLoader

skipped_formatting = cfg.get("formatting", "skip", fallback=[])
dry_run_function = cfg.get("dry_run", "function", fallback=None)
schema_config_dict = cfg.get("schema")
```

The `ConfigLoader.get()` method allows multiple string parameters to reference a configuration value that is stored in a  nested structure. A `fallback` value can be optionally provided in case the configuration parameter is not set.

## Adding configuration parameters

New configuration parameters can simply be added to `bqetl_project.yaml`. `ConfigLoader.get()` allows for these new parameters simply to be referenced without needing to be changed or updated.
