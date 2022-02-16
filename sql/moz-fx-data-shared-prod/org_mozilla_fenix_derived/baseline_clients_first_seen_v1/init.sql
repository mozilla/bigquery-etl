-- Generated via bigquery_etl.glean_usage
CREATE TABLE IF NOT EXISTS
  `org_mozilla_fenix_derived.baseline_clients_first_seen_v1`
PARTITION BY
  first_seen_date
CLUSTER BY
  sample_id,
  submission_date
OPTIONS
  (require_partition_filter = FALSE)
AS
WITH baseline AS (
  SELECT
    client_info.client_id,
      -- Some Glean data from 2019 contains incorrect sample_id, so we
      -- recalculate here; see bug 1707640
    udf.safe_sample_id(client_info.client_id) AS sample_id,
    DATE(MIN(submission_timestamp)) AS submission_date,
    DATE(MIN(submission_timestamp)) AS first_seen_date,
  FROM
    `org_mozilla_fenix_stable.baseline_v1`
    -- initialize by looking over all of history
  WHERE
    DATE(submission_timestamp) > "2010-01-01"
  GROUP BY
    client_id,
    sample_id
),
-- this lookup is ~13GB on release (org_mozilla_firefox) as of 2021-03-31
_fennec_id_lookup AS (
  SELECT
    client_info.client_id,
    MIN(metrics.uuid.migration_telemetry_identifiers_fennec_client_id) AS fennec_client_id
  FROM
    `org_mozilla_fenix_stable.migration_v1`
  WHERE
    DATE(submission_timestamp) > "2010-01-01"
    AND client_info.client_id IS NOT NULL
    AND metrics.uuid.migration_telemetry_identifiers_fennec_client_id IS NOT NULL
  GROUP BY
    1
),
_core AS (
  SELECT
    *
  FROM
    `telemetry_derived.core_clients_first_seen_v1`
  WHERE
    first_seen_date > "2010-01-01"
),
-- scanning this table is ~25GB
_core_clients_first_seen AS (
  SELECT
    _fennec_id_lookup.client_id,
    first_seen_date
  FROM
    _fennec_id_lookup
  JOIN
    _core
  ON
    _fennec_id_lookup.fennec_client_id = _core.client_id
)
SELECT
  client_id,
  submission_date,
  COALESCE(core.first_seen_date, baseline.first_seen_date) AS first_seen_date,
  sample_id
FROM
  baseline
LEFT JOIN
  _core_clients_first_seen core
USING
  (client_id)
