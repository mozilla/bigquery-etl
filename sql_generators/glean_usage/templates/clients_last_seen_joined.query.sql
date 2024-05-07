{{ header }}
{% set products_to_include_extra_activity_fields = [
  "firefox_ios",
  "focus_ios",
  "klar_ios",
  "fenix",
  "focus_android",
  "klar_android",
  "firefox_desktop"
] %}

WITH baseline AS (
  SELECT
    * {% if app_name in products_to_include_extra_activity_fields %}EXCEPT(
    -- These were added as part of https://mozilla-hub.atlassian.net/browse/DENG-3462
    -- to enable KPI metric calculations. This exclusion is to avoid potential compatibility
    -- issues downstream from this query.
    is_dau,
    is_wau,
    is_mau,
    is_daily_user,
    is_weekly_user,
    is_monthly_user,
    app_name,
    activity_segment,
    is_mobile,
    is_desktop
  ){% endif %}
  FROM
    `{{ project_id }}.{{ app_name }}.baseline_clients_last_seen`
  WHERE
    {% raw %}{% if is_init() %}{% endraw %}
    FALSE
    {% raw %}{% else %}{% endraw %}
    submission_date = @submission_date
    {% raw %}{% endif %}{% endraw %}
),
metrics AS (
  SELECT
    *
  FROM
    `{{ project_id }}.{{ app_name }}.metrics_clients_last_seen`
  WHERE
    {% raw %}{% if is_init() %}{% endraw %}
    FALSE
    {% raw %}{% else %}{% endraw %}
    -- The join between baseline and metrics pings is based on submission_date with a 1 day delay,
    -- since metrics pings usually arrive within 1 day after their logical activity period.
    submission_date = DATE_ADD(@submission_date, INTERVAL 1 DAY)
    {% raw %}{% endif %}{% endraw %}
)
SELECT
  baseline.client_id,
  baseline.sample_id,
  baseline.submission_date,
  baseline.normalized_channel,
  * EXCEPT(submission_date, normalized_channel, client_id, sample_id)
FROM
  baseline
LEFT JOIN metrics
ON baseline.client_id = metrics.client_id AND
  baseline.sample_id = metrics.sample_id AND
  (
    baseline.normalized_channel = metrics.normalized_channel OR
    (baseline.normalized_channel IS NULL AND metrics.normalized_channel IS NULL)
  )
