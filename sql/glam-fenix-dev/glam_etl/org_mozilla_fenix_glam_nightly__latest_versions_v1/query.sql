-- query for org_mozilla_fenix_glam_nightly__latest_versions_v1;
WITH extracted AS (
  SELECT
    client_id,
    channel,
    app_version
  FROM
    glam_etl.org_mozilla_fenix_glam_nightly__view_clients_daily_scalar_aggregates_v1
  WHERE
    submission_date
    BETWEEN DATE_SUB(@submission_date, INTERVAL 28 DAY)
    AND @submission_date
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
    COUNT(DISTINCT client_id) > 5
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
