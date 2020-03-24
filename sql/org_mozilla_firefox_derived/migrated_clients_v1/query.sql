WITH migrations AS (
  SELECT
    client_info.client_id AS fenix_client_id,
    metrics.uuid.migration_telemetry_identifiers_fennec_client_id AS fennec_client_id,
    submission_date,
    normalized_channel,
    versions.value AS value
  FROM
    `moz-fx-data-shared-prod.org_mozilla_firefox.migration`
  LEFT JOIN
    UNNEST(metrics.labeled_string.migration_migration_versions) versions
  WHERE
    submission_date = @submission_date
),
clients AS (
  SELECT
    fenix_client_id,
    submission_date,
    normalized_channel,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(fennec_client_id IGNORE NULLS)
    ) AS fennec_client_id,
    COUNT(*) AS migration_ping_count
  FROM
    migrations
  GROUP BY
    fenix_client_id,
    submission_date,
    normalized_channel
)
SELECT
  fenix_client_id,
  COALESCE(CURRENT.fennec_client_id, prev.fennec_client_id) AS fennec_client_id,
  COALESCE(CURRENT.normalized_channel, prev.normalized_channel) AS normalized_channel,
  COALESCE(CURRENT.submission_date, prev.submission_date) AS submission_date,
  prev.migration_ping_count + CURRENT.migration_ping_count AS migration_ping_count
FROM
  clients CURRENT
FULL OUTER JOIN
  org_mozilla_firefox_derived.migrated_clients_v1
ON
  (fenix_client_id)
