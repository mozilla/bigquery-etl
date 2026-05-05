# Public Data

For background, see [Accessing Public Data](https://docs.telemetry.mozilla.org/cookbooks/public_data.html)
on `docs.telemetry.mozilla.org`.

- To make query results publicly available, the `public_bigquery` flag must be set in
  `metadata.yaml`
  - Tables will get published in the `mozilla-public-data` GCP project which is accessible
    by everyone, also external users
- To make query results publicly available as JSON, `public_json` flag must be set in
  `metadata.yaml`
  - Data will be accessible under https://public-data.telemetry.mozilla.org
    - A list of all available datasets is published under https://public-data.telemetry.mozilla.org/all-datasets.json
  - For example: https://public-data.telemetry.mozilla.org/api/v1/tables/telemetry_derived/ssl_ratios/v1/files/000000000000.json
  - Output JSON files have a maximum size of 1GB, data can be split up into multiple files (`000000000000.json`, `000000000001.json`, ...)
  - `incremental_export` controls how data should be exported as JSON:
    - `false`: all data of the source table gets exported to a single location
      - https://public-data.telemetry.mozilla.org/api/v1/tables/telemetry_derived/ssl_ratios/v1/files/000000000000.json
    - `true`: only data that matches the `submission_date` parameter is exported as JSON to a separate directory for this date
      - https://public-data.telemetry.mozilla.org/api/v1/tables/telemetry_derived/ssl_ratios/v1/files/2020-03-15/000000000000.json
- For each dataset, a `metadata.json` gets published listing all available files, for example: https://public-data.telemetry.mozilla.org/api/v1/tables/telemetry_derived/ssl_ratios/v1/files/metadata.json
- The timestamp when the dataset was last updated is recorded in `last_updated`, e.g.: https://public-data.telemetry.mozilla.org/api/v1/tables/telemetry_derived/ssl_ratios/v1/last_updated
