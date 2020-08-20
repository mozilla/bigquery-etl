-- Example of a query over all Fenix builds advertised as "Firefox Beta"
CREATE TEMP FUNCTION extract_fields(app_id STRING, m ANY TYPE) AS (
  (
    SELECT AS STRUCT
      m.submission_timestamp,
      m.metrics.string.geckoview_version,
      mozfun.norm.fenix_app_info(app_id, m.client_info.app_build).*
  )
);

WITH base AS (
  SELECT
    extract_fields('org_mozilla_firefox_beta', m).*
  FROM
    org_mozilla_firefox_beta.metrics AS m
  UNION ALL
  SELECT
    extract_fields('org_mozilla_fenix', m).*
  FROM
    org_mozilla_fenix.metrics AS m
)
SELECT
  DATE(submission_timestamp) AS submission_date,
  geckoview_version,
  COUNT(*)
FROM
  base
WHERE
  app_name = 'Fenix'  -- excludes 'Firefox Preview'
  AND channel = 'beta'
  AND DATE(submission_timestamp) = '2020-08-01'
GROUP BY
  submission_date,
  geckoview_version
