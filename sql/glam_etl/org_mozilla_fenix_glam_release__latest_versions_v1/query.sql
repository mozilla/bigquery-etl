-- query for org_mozilla_fenix_glam_release__latest_versions_v1;
WITH extracted AS (
  SELECT
    client_id,
    channel,
    app_version
  FROM
    glam_etl.org_mozilla_fenix_glam_release__view_clients_daily_scalar_aggregates_v1
  WHERE
    submission_date > DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND channel IS NOT NULL
),
transformed AS (
  SELECT
    channel,
    app_version
  FROM
    extracted
  GROUP BY
    channel,
    app_version
  HAVING
    COUNT(DISTINCT client_id) > 100
  ORDER BY
    channel,
    app_version DESC
)
SELECT
  channel,
  MAX(app_version) AS latest_version
FROM
  transformed
GROUP BY
  channel
