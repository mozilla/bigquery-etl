CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod.org_mozilla_firefox_derived.migrated_clients_v1`
PARTITION BY
  (submission_date)
CLUSTER BY
  (fenix_client_id)
AS
WITH migrations AS (
  SELECT
    client_info.client_id AS fenix_client_id,
    metrics.uuid.migration_telemetry_identifiers_fennec_client_id AS fennec_client_id,
    submission_date,
    normalized_channel,
    client_info.device_manufacturer,
    metadata.geo.country,
    versions.value AS value
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox.migration`
  LEFT JOIN
    UNNEST(metrics.labeled_string.migration_migration_versions) versions
  WHERE
    submission_date
    BETWEEN '2020-01-01'
    AND current_date
),
clients AS (
  SELECT
    fenix_client_id,
    MAX(submission_date) AS submission_date,
    udf.mode_last(ARRAY_AGG(normalized_channel IGNORE NULLS)) AS normalized_channel,
    udf.mode_last(ARRAY_AGG(device_manufacturer IGNORE NULLS)) AS manufacturer,
    udf.mode_last(ARRAY_AGG(country IGNORE NULLS)) AS country,
    udf.mode_last(ARRAY_AGG(fennec_client_id IGNORE NULLS)) AS fennec_client_id,
    COUNT(*) AS migration_ping_count
  FROM
    migrations
  GROUP BY
    fenix_client_id
)
SELECT
  *
FROM
  clients
WHERE
  fenix_client_id != 'c0ffeec0-ffee-c0ff-eec0-ffeec0ffeec0'
  AND fennec_client_id != 'c0ffeec0-ffee-c0ff-eec0-ffeec0ffeec0'
