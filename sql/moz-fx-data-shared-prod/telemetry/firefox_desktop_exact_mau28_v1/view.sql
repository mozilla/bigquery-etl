CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.firefox_desktop_exact_mau28_v1`
AS
WITH base AS (
  SELECT
    submission_date,
    SUM(mau) AS mau,
    SUM(wau) AS wau,
    SUM(dau) AS dau,
    SUM(IF(country IN ('US', 'GB', 'FR', 'DE', 'CA'), mau, 0)) AS tier1_mau,
    SUM(IF(country IN ('US', 'GB', 'FR', 'DE', 'CA'), wau, 0)) AS tier1_wau,
    SUM(IF(country IN ('US', 'GB', 'FR', 'DE', 'CA'), dau, 0)) AS tier1_dau,
    SUM(visited_5_uri_mau) AS visited_5_uri_mau,
    SUM(visited_5_uri_wau) AS visited_5_uri_wau,
    SUM(visited_5_uri_dau) AS visited_5_uri_dau
  FROM
    `moz-fx-data-shared-prod.telemetry_derived.firefox_desktop_exact_mau28_by_dimensions_v1`
  GROUP BY
    submission_date
),
  -- We use imputed values for the period after the Armag-add-on deletion event;
  -- see https://bugzilla.mozilla.org/show_bug.cgi?id=1552558
imputed_global AS (
  SELECT
    `date` AS submission_date,
    mau
  FROM
    `moz-fx-data-shared-prod.static.firefox_desktop_imputed_mau28_v1`
  WHERE
    datasource = 'desktop_global'
),
    --
imputed_tier1 AS (
  SELECT
    `date` AS submission_date,
    mau
  FROM
    `moz-fx-data-shared-prod.static.firefox_desktop_imputed_mau28_v1`
  WHERE
    datasource = 'desktop_tier1'
)
SELECT
  base.* REPLACE (
    COALESCE(imputed_global.mau, base.mau) AS mau,
    COALESCE(imputed_tier1.mau, base.tier1_mau) AS tier1_mau
  )
FROM
  base
FULL JOIN
  imputed_global
  USING (submission_date)
FULL JOIN
  imputed_tier1
  USING (submission_date)
ORDER BY
  submission_date
