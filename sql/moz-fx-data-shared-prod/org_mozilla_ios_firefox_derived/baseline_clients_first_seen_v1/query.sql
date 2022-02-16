-- Generated via bigquery_etl.glean_usage
WITH _current AS (
  SELECT DISTINCT
    @submission_date AS submission_date,
    @submission_date AS first_seen_date,
    sample_id,
    client_info.client_id
  FROM
    `org_mozilla_ios_firefox_stable.baseline_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
),
  -- query over all of history to see whether the client_id has shown up before
_previous AS (
  SELECT
    submission_date,
    first_seen_date,
    sample_id,
    client_id
  FROM
    `org_mozilla_ios_firefox_derived.baseline_clients_first_seen_v1`
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
