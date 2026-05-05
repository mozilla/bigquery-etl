{% if is_init() %}
  SELECT
    client_id,
    DATE(MIN(submission_timestamp)) AS first_seen_date,
  FROM
    `moz-fx-data-shared-prod.telemetry.core`
  WHERE
    submission_timestamp > '2010-01-01'
  GROUP BY
    client_id
{% else %}
  WITH _current AS (
    SELECT DISTINCT
      client_id
    FROM
      `moz-fx-data-shared-prod.telemetry.core`
    WHERE
      DATE(submission_timestamp) = @submission_date
  ),
  _previous AS (
    SELECT
      *
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.core_clients_first_seen_v1`
    WHERE
    -- In the case we need to backfill older partitions of this table, we don't want newer partitions
    -- to alter results of the query.
      first_seen_date < @submission_date
  )
  SELECT
    client_id,
    @submission_date AS first_seen_date
  FROM
    _current
  LEFT JOIN
    _previous
    USING (client_id)
  WHERE
    _previous.client_id IS NULL
{% endif %}
