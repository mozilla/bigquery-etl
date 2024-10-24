{{ header }}

{% raw %}
{% if is_init() %}
{% endraw %}
WITH
  base AS (
    SELECT
      client_info.client_id,
      -- dau_id,
      DATE(MIN(submission_timestamp)) AS submission_date,
      DATE(MIN(submission_timestamp)) AS first_seen_date,
    FROM
      `{{ dau_reporting_stable_table }}`
    WHERE
      DATE(submission_timestamp) > "2014-10-10"
    GROUP BY
      client_id
  )
{% raw %}
{% else %}
{% endraw %}
WITH _current AS (
  SELECT DISTINCT
    @submission_date as submission_date,
    @submission_date as first_seen_date,
    -- dau_id,
    client_info.client_id,
  FROM
    `{{ dau_reporting_stable_table }}`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
),
  -- query over all of history to see whether the client_id has shown up before
_previous AS (
  SELECT
    submission_date,
    first_seen_date,
    -- dau_id,
    client_id,
  FROM
    `{{ dau_reporting_clients_first_seen_table }}`
  WHERE
    first_seen_date > "2014-10-10"
    AND first_seen_date < @submission_date
)
{% raw %}
{% endif %}
{% endraw %}

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
    USING (client_id)
)

SELECT
  submission_date,
  first_seen_date,
  -- dau_id,
  client_id,
FROM _joined
QUALIFY
  IF(
    COUNT(*) OVER (PARTITION BY client_id) > 1,
    ERROR("duplicate client_id detected"),
    TRUE
  )

{% raw %}
{% endif %}
{% endraw %}
