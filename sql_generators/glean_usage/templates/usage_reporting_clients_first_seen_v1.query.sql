{{ header }}

WITH
  _current AS (
    SELECT
      usage_profile_id,
      {% raw %}
      {% if is_init() %}
      DATE(MIN(submission_timestamp)) AS first_seen_date,
      {% else %}
      @submission_date AS first_seen_date,
      {% endif %}
      {% endraw %}
    FROM
      `{{ usage_reporting_clients_daily_table }}`
    WHERE
      usage_profile_id IS NOT NULL
      {% raw %}
      {% if is_init() %}
      AND DATE(submission_timestamp) > "2014-10-10"
      {% else %}
      AND DATE(submission_timestamp) = @submission_date
      {% endif %}
      {% endraw %}
    GROUP BY
      usage_profile_id
  ),
_previous AS (
  SELECT
    usage_profile_id,
  FROM
    `{{ usage_reporting_clients_first_seen_table }}`
  WHERE
    {% raw %}
    {% if is_init() %}
    False
    {% else %}
    first_seen_date < @submission_date
    {% endif %}
    {% endraw %}
)

SELECT
  first_seen_date,
  usage_profile_id,
FROM
  _current
LEFT JOIN
  _previous
  USING (usage_profile_id)
WHERE
  _previous.usage_profile_id IS NULL
QUALIFY
  IF(
    COUNT(*) OVER (PARTITION BY usage_profile_id) > 1,
    ERROR("Duplicate usage_profile_id combination detected."),
    TRUE
  )
