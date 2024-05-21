--- Query generated via sql_generators.active_users.
WITH metrics AS (
    -- Metrics ping can arrive either in the same or next day as the baseline ping.
  WITH min_metrics_ping AS (
    SELECT
      client_id,
      MIN(submission_date) AS submission_date
    FROM
      `{{ project_id }}.{{ app_name }}.metrics_clients_last_seen`
    WHERE
      submission_date
      BETWEEN @submission_date
      AND DATE_ADD(@submission_date, INTERVAL 1 DAY)
    GROUP BY
      client_id
  )
  SELECT
    client_id,
    submission_date,
    metrics.normalized_channel AS channel,
    {% if app_name == "klar_android"%}
      CAST(NULL AS INTEGER) AS uri_count,
      CAST(NULL AS INTEGER) AS is_default_browser
    {% else %}
      metrics.uri_count AS uri_count,
      metrics.is_default_browser AS is_default_browser
    {% endif %}
  FROM
    `{{ project_id }}.{{ app_name }}.metrics_clients_last_seen` AS metrics
  INNER JOIN
    min_metrics_ping
    USING (client_id, submission_date)
  WHERE
    submission_date
    BETWEEN @submission_date
    AND DATE_ADD(@submission_date, INTERVAL 1 DAY)
),
baseline AS (
  SELECT
    client_id,
    app_display_version AS app_version,
    IFNULL(country, '??') AS country,
    device_model,
    EXTRACT(YEAR FROM first_seen_date) AS first_seen_year,
    app_name,
    normalized_channel AS channel,
    normalized_os AS os,
    normalized_os_version AS os_version,
    COALESCE(
      SAFE_CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(0)], "") AS INTEGER),
      0
    ) AS os_version_major,
    COALESCE(
      SAFE_CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(1)], "") AS INTEGER),
      0
    ) AS os_version_minor,
    COALESCE(
      SAFE_CAST(NULLIF(SPLIT(normalized_os_version, ".")[SAFE_OFFSET(2)], "") AS INTEGER),
      0
    ) AS os_version_patch,
    submission_date,
    is_daily_user,
    is_weekly_user,
    is_monthly_user,
    is_dau,
    is_wau,
    is_mau
  FROM
    `{{ project_id }}.{{ app_name }}.baseline_clients_last_seen` AS baseline
  WHERE
    submission_date = @submission_date
),
unioned AS (
  SELECT
    baseline.*,
    metrics.uri_count,
    metrics.is_default_browser
  FROM
    baseline
  LEFT JOIN
    metrics
    ON (
      baseline.client_id = metrics.client_id
      AND baseline.channel IS NOT DISTINCT FROM metrics.channel
    )
)
SELECT
  unioned.* EXCEPT (
    client_id,
    is_daily_user,
    is_weekly_user,
    is_monthly_user,
    is_dau,
    is_wau,
    is_mau,
    uri_count
  ),
  COUNTIF(is_daily_user) AS daily_users,
  COUNTIF(is_weekly_user) AS weekly_users,
  COUNTIF(is_monthly_user) AS monthly_users,
  COUNTIF(is_dau) AS dau,
  COUNTIF(is_wau) AS wau,
  COUNTIF(is_mau) AS mau,
  SUM(uri_count) AS uri_count,
  CAST(NULL AS FLOAT64) AS active_hours,
FROM
  unioned
GROUP BY
  ALL
