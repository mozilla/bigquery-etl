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
_baseline AS (
  -- extract the client_id into the top level for the `USING` clause
  SELECT DISTINCT
    client_info.client_id,
    -- Some Glean data from 2019 contains incorrect sample_id, so we
    -- recalculate here; see bug 1707640
    udf.safe_sample_id(client_info.client_id) AS sample_id,
  FROM
    `org_mozilla_fenix_stable.baseline_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
),
_current AS (
  SELECT DISTINCT
    @submission_date AS submission_date,
    COALESCE(first_seen_date, @submission_date) AS first_seen_date,
    sample_id,
    client_id
  FROM
    _baseline
  LEFT JOIN
    _core_clients_first_seen
  USING
    (client_id)
),
_previous AS (
  SELECT
    fs.submission_date,
    IF(
      core IS NOT NULL
      AND core.first_seen_date <= fs.first_seen_date,
      core.first_seen_date,
      fs.first_seen_date
    ) AS first_seen_date,
    sample_id,
    client_id
  FROM
    `org_mozilla_fenix_derived.baseline_clients_first_seen_v1` fs
  LEFT JOIN
    _core_clients_first_seen core
  USING
    (client_id)
  WHERE
    fs.first_seen_date > "2010-01-01"
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
