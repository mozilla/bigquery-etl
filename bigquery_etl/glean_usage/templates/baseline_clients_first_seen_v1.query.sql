{{ header }}
{% from "macros.sql" import core_clients_first_seen %}

WITH
{% if fennec_id %}
{{ core_clients_first_seen(migration_table) }},
_current AS (
  SELECT
    coalesce(first_seen_date, @submission_date) as first_seen_date,
    sample_id,
    client_info.client_id
  FROM
    `{{ baseline_table }}`
  LEFT JOIN
    _core_clients_first_seen
  USING
    (client_id)
  WHERE
    DATE(submission_timestamp) = @submission_date
),
{% else %}
_current AS (
  SELECT DISTINCT
    @submission_date as first_seen_date,
    sample_id,
    client_info.client_id
  FROM
    `{{ baseline_table }}`
  WHERE
    DATE(submission_timestamp) = @submission_date
    and client_info.client_id IS NOT NULL
),
{% endif %}
  -- query over all of history to see whether the client_id has shown up before
_previous AS (
  SELECT
    *
  FROM
    `{{ first_seen_table }}`
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
