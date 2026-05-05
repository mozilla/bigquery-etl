CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.topsites_rate_fenix_live`
AS
WITH base AS (
  SELECT
    *,
    'release' AS channel,
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.topsites_rate_fenix_release_live_v1`
  UNION ALL
  SELECT
    *,
    'beta' AS channel,
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.topsites_rate_fenix_beta_live_v1`
  UNION ALL
  SELECT
    *,
    'nightly' AS channel
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.topsites_rate_fenix_nightly_live_v1`
)
SELECT
  submission_minute,
  SUM(n_click) AS n_click,
  SUM(n_impression) AS n_impression,
  SUM(IF(channel = 'release', n_click, 0)) AS n_click_release,
  SUM(IF(channel = 'release', n_impression, 0)) AS n_impression_release,
FROM
  base
GROUP BY
  submission_minute
