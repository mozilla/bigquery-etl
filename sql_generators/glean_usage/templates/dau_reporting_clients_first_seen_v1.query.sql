{{ header }}

WITH
  _current AS (
    SELECT
      client_info.client_id,
      -- TODO: Once this field exists we should update the reference.
      CAST(NULL AS STRING) AS usage_profile_id,
      {% raw %}
      {% if is_init() %}
      DATE(MIN(submission_timestamp)) AS first_seen_date,
      {% else %}
      @submission_date AS first_seen_date,
      {% endif %}
      {% endraw %}
    FROM
      `{{ dau_reporting_stable_table }}`
    WHERE
      client_info.client_id IS NOT NULL
      {% raw %}
      {% if is_init() %}
      AND DATE(submission_timestamp) > "2014-10-10"
      {% else %}
      AND DATE(submission_timestamp) = @submission_date
      {% endif %}
      {% endraw %}
    GROUP BY
      client_id
  ),
_previous AS (
  SELECT
    client_id,
    usage_profile_id,
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
)

SELECT
  first_seen_date,
  client_id,
  usage_profile_id,
FROM
  _previous
LEFT JOIN
  _current
  USING (client_id, usage_profile_id)
WHERE
  _previous.client_id IS NULL
  AND _previous.usage_profile_id IS NULL
QUALIFY
  IF(
    COUNT(*) OVER (PARTITION BY client_id, usage_profile_id) > 1,
    ERROR("Duplicate client_id, usage_profile_id combination detected."),
    TRUE
  )