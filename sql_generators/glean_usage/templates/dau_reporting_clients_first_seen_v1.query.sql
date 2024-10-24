{{ header }}

WITH
  _current AS (
    SELECT
      client_info.client_id,
      -- dau_id,
      {% raw %}
      {% if is_init() %}
      DATE(MIN(submission_timestamp)) AS submission_date,
      DATE(MIN(submission_timestamp)) AS first_seen_date,
      {% else %}
      @submission_date AS submission_date,
      @submission_date AS first_seen_date,
      {% endif %}
      {% endraw %}
      client_info.client_id,
    FROM
      `{{ dau_reporting_stable_table }}`
    WHERE
      {% raw %}
      {% if is_init() %}
      DATE(submission_timestamp) > "2014-10-10"
      {% else %}
      DATE(submission_timestamp) = @submission_date
      {% endif %}
      {% endraw %}
      AND client_info.client_id IS NOT NULL
    GROUP BY
      client_id
  ),
_previous AS (
  SELECT
    submission_date,
    first_seen_date,
    -- dau_id,
    client_id,
  FROM
    `{{ dau_reporting_clients_first_seen_table }}`
  WHERE
    {% raw %}
    {% if is_init() %}
    False
    {% else %}
    first_seen_date < @submission_date
    {% endif %}
    {% endraw %}
),

_joined AS (
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
