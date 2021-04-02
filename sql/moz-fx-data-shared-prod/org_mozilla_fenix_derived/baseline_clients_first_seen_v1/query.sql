-- Generated via bigquery_etl.glean_usage
WITH
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
),
_current AS (
  SELECT
    coalesce(first_seen_date, @submission_date) AS first_seen_date,
    sample_id,
    client_info.client_id
  FROM
    `org_mozilla_fenix_stable.baseline_v1`
  LEFT JOIN
    _core_clients_first_seen
  USING
    (client_id)
  WHERE
    DATE(submission_timestamp) = @submission_date
),
  -- query over all of history to see whether the client_id has shown up before
_previous AS (
  SELECT
    *
  FROM
    `org_mozilla_fenix_derived.baseline_clients_first_seen_v1`
  WHERE
    first_seen_date > "2010-01-01"
)
  --
SELECT
  IF(
    _previous.client_id IS NULL
    OR _previous.first_seen_date >= _current.first_seen_date,
    _current,
    _previous
  ).*
FROM
  _current
FULL JOIN
  _previous
USING
  (client_id)
