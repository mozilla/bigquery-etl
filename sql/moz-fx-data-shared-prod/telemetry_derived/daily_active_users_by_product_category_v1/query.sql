SELECT
  submission_date,
  CASE
    WHEN app_name = "Firefox Desktop"
      THEN "desktop"
    WHEN app_name IN (
        "Fenix",
        "Firefox iOS",
        "Focus Android",
        "Focus iOS",
        "focus_android",
        "focus_ios"
      )
      THEN "mobile"
    ELSE "other"
  END AS product_category,
  SUM(dau) AS dau
FROM
  `moz-fx-data-shared-prod.telemetry.active_users_aggregates`
WHERE
  {% if is_init() %}
    submission_date >= "2023-12-01"
  {% else %}
    submission_date = @submission_date
  {% endif %}
GROUP BY
  submission_date,
  product_category
