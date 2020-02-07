/*

This clients_first_seen table is incrementally populated every day such that
each client only appears once in the entire table. For each day's query, we
scan the entire history of the table to exclude clients we've already recorded
as seen.

This table is convenient for retention calculations where we want to be able to
tell the difference between a lapsed user returning and a truly new user.

*/
-- We union over baseline pings and migration pings so that we can pull the
-- user's fennec user ID from the migration ping if they are being upgraded
-- from fennec.
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
per_client AS (
  SELECT
    client_id,
    DATE(MIN(submission_timestamp)) AS fenix_first_seen_date,
    udf.mode_last(ARRAY_AGG(LOWER(fennec_id))) AS fennec_id,
  FROM
    base
  WHERE
    submission_timestamp = @submission_timestamp
  GROUP BY
    client_id
)
SELECT
  per_client.client_id,
  per_client.fenix_first_seen_date,
  core_clients_first_seen.first_seen_date AS fennec_first_seen_date,
FROM
  per_client
LEFT JOIN
  telemetry_derived.core_clients_first_seen_v1 AS core_clients_first_seen
ON
  (fennec_id = core_clients_first_seen.client_id)
