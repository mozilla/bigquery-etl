CREATE OR REPLACE VIEW `moz-fx-data-shared-prod.org_mozilla_firefox.migrated_clients_v1` AS
WITH migrations AS
(
--Fennec Nightly
SELECT
    client_info.client_id AS fenix_client_id,
    metrics.uuid.migration_telemetry_identifiers_fennec_client_id AS fennec_client_id,
    DATE(submission_timestamp) AS submission_date,
    'nightly' AS normalized_channel,
    versions.value AS value
FROM
  `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.migration_v1`
LEFT JOIN
  UNNEST (metrics.labeled_string.migration_migration_versions) versions

--Fennec Beta
UNION ALL
SELECT
    client_info.client_id AS fenix_client_id,
    metrics.uuid.migration_telemetry_identifiers_fennec_client_id AS fennec_client_id,
    DATE(submission_timestamp) AS submission_date,
    'beta' AS normalized_channel,
    versions.value AS value
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.migration_v1`
LEFT JOIN
  UNNEST (metrics.labeled_string.migration_migration_versions) versions

--Fennec Release
UNION ALL
SELECT
    client_info.client_id AS fenix_client_id,
    metrics.uuid.migration_telemetry_identifiers_fennec_client_id AS fennec_client_id,
    DATE(submission_timestamp) AS submission_date,
    'release' AS normalized_channel,
    versions.value AS value
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_stable.migration_v1`
LEFT JOIN
  UNNEST (metrics.labeled_string.migration_migration_versions) versions
),

clients AS
(
  SELECT
    fenix_client_id,
    submission_date,
    normalized_channel,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(fennec_client_id IGNORE NULLS)) AS fennec_client_id,
    COUNT(*) AS migration_ping_count
  FROM
    migrations
  GROUP BY
    fenix_client_id,
    submission_date,
    normalized_channel
)

SELECT
  *
FROM
  clients
WHERE
  fenix_client_id != 'c0ffeec0-ffee-c0ff-eec0-ffeec0ffeec0'
  AND fennec_client_id != 'c0ffeec0-ffee-c0ff-eec0-ffeec0ffeec0'
