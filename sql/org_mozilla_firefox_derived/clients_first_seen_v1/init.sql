-- See query.sql for more explanation of what's going on here.
CREATE TABLE
  `moz-fx-data-shared-prod.org_mozilla_firefox_derived.clients_first_seen_v1`
PARTITION BY
  (fenix_first_seen_date)
AS
WITH base AS (
  SELECT
    client_info.client_id,
    submission_timestamp,
    CAST(NULL AS STRING) AS fennec_id
  FROM
    org_mozilla_firefox.baseline
  UNION ALL
  SELECT
    client_info.client_id,
    submission_timestamp,
    CAST(NULL AS STRING) AS fennec_id
  FROM
    org_mozilla_firefox_beta.baseline
  UNION ALL
  SELECT
    client_info.client_id,
    submission_timestamp,
    CAST(NULL AS STRING) AS fennec_id
  FROM
    org_mozilla_fennec_aurora.baseline
  UNION ALL
  SELECT
    client_info.client_id,
    submission_timestamp,
    CAST(NULL AS STRING) AS fennec_id
  FROM
    org_mozilla_fenix.baseline
  UNION ALL
  SELECT
    client_info.client_id,
    submission_timestamp,
    CAST(NULL AS STRING) AS fennec_id
  FROM
    org_mozilla_fenix_nightly.baseline
  UNION ALL
  SELECT
    client_info.client_id,
    submission_timestamp,
    metrics.uuid.migration_telemetry_identifiers_fennec_client_id AS fennec_id
  FROM
    org_mozilla_firefox.migration
  UNION ALL
  SELECT
    client_info.client_id,
    submission_timestamp,
    metrics.uuid.migration_telemetry_identifiers_fennec_client_id AS fennec_id
  FROM
    org_mozilla_firefox_beta.migration
  UNION ALL
  SELECT
    client_info.client_id,
    submission_timestamp,
    metrics.uuid.migration_telemetry_identifiers_fennec_client_id AS fennec_id
  FROM
    org_mozilla_fennec_aurora.migration
  UNION ALL
  SELECT
    client_info.client_id,
    submission_timestamp,
    metrics.uuid.migration_telemetry_identifiers_fennec_client_id AS fennec_id
  FROM
    org_mozilla_fenix.migration
  UNION ALL
  SELECT
    client_info.client_id,
    submission_timestamp,
    metrics.uuid.migration_telemetry_identifiers_fennec_client_id AS fennec_id
  FROM
    org_mozilla_fenix_nightly.migration
),
--
per_client_id AS (
  SELECT
    client_id,
    DATE(MIN(submission_timestamp)) AS fenix_first_seen_date,
    udf.mode_last(ARRAY_AGG(LOWER(fennec_id))) AS fennec_id,
  FROM
    base
  WHERE
    submission_timestamp > '2010-01-01'
  GROUP BY
    client_id
)
--
SELECT
  per_client_id.client_id,
  per_client_id.fenix_first_seen_date,
  core_clients_first_seen.first_seen_date AS fennec_first_seen_date,
FROM
  per_client_id
LEFT JOIN
  telemetry_derived.core_clients_first_seen_v1 AS core_clients_first_seen
ON
  (fennec_id = core_clients_first_seen.client_id)
