-- Generated via `usage_reporting` SQL generator.
WITH _current AS (
  SELECT
    usage_profile_id,
    {% if is_init() %}
      MIN(submission_date) AS first_seen_date,
    {% else %}
      @submission_date AS first_seen_date,
    {% endif %}
    app_channel,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix.usage_reporting_clients_daily`
  WHERE
    usage_profile_id IS NOT NULL
    {% if is_init() %}
      AND submission_date > "2014-10-10"
    {% else %}
      AND submission_date = @submission_date
    {% endif %}
  GROUP BY
    usage_profile_id,
    app_channel
),
_previous AS (
  SELECT
    usage_profile_id,
    app_channel,
  FROM
    `moz-fx-data-shared-prod.org_mozilla_fenix_derived.usage_reporting_clients_first_seen_v1`
  WHERE
    {% if is_init() %}
      FALSE
    {% else %}
      first_seen_date < @submission_date
    {% endif %}
)
SELECT
  first_seen_date,
  usage_profile_id,
  app_channel,
FROM
  _current
LEFT JOIN
  _previous
  USING (usage_profile_id, app_channel)
WHERE
  _previous.usage_profile_id IS NULL
QUALIFY
  IF(
    COUNT(*) OVER (PARTITION BY usage_profile_id) > 1,
    ERROR("Duplicate usage_profile_id combination detected."),
    TRUE
  )
