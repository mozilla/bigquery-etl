{{ header }}
{% from "macros.sql" import core_clients_first_seen %}

WITH
{% if fennec_id %}
{{ core_clients_first_seen(migration_table) }},
_baseline AS (
  -- extract the client_id into the top level for the `USING` clause
  SELECT DISTINCT
    client_info.client_id,
    -- Some Glean data from 2019 contains incorrect sample_id, so we
    -- recalculate here; see bug 1707640
    udf.safe_sample_id(client_info.client_id) AS sample_id,
  FROM
    `{{ baseline_table }}`
  WHERE
    DATE(submission_timestamp) = @submission_date
),
_current AS (
  SELECT DISTINCT
    @submission_date as submission_date,
    COALESCE(first_seen_date, @submission_date) as first_seen_date,
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
      core IS NOT NULL AND core.first_seen_date <= fs.first_seen_date,
      core.first_seen_date,
      fs.first_seen_date
    ) AS first_seen_date,
    sample_id,
    client_id
  FROM
    `{{ first_seen_table }}` fs
  LEFT JOIN
    _core_clients_first_seen core
  USING
    (client_id)
  WHERE
    fs.first_seen_date > "2010-01-01"
    AND fs.first_seen_date < @submission_date
)
{% else %}
_current AS (
  SELECT DISTINCT
    @submission_date as submission_date,
    @submission_date as first_seen_date,
    sample_id,
    client_info.client_id
  FROM
    `{{ baseline_table }}`
  WHERE
    DATE(submission_timestamp) = @submission_date
    and client_info.client_id IS NOT NULL
),
  -- query over all of history to see whether the client_id has shown up before
_previous AS (
  SELECT
    submission_date,
    first_seen_date,
    sample_id,
    client_id
  FROM
    `{{ first_seen_table }}`
  WHERE
    first_seen_date > "2010-01-01"
    AND first_seen_date < @submission_date
)
{% endif %}

, _joined AS (
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
)

-- added this as the result of bug#1788650
SELECT
  submission_date,
  first_seen_date,
  sample_id,
  client_id
FROM _joined
QUALIFY
  IF(
    COUNT(*) OVER (PARTITION BY client_id) > 1,
    ERROR("duplicate client_id detected"),
    TRUE
  )
