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
  COALESCE(_current.fennec_client_id, _previous.fennec_client_id) AS fennec_client_id,
  COALESCE(_current.normalized_channel, _previous.normalized_channel) AS normalized_channel,
  COALESCE(_current.submission_date, _previous.submission_date) AS submission_date,
  _previous.migration_ping_count + _current.migration_ping_count AS migration_ping_count
FROM
  clients _current
FULL OUTER JOIN
  org_mozilla_firefox_derived.migrated_clients_v1 _previous
ON
  (fenix_client_id)
