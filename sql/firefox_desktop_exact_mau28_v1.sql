WITH
  base AS (
  SELECT
    submission_date,
    SUM(mau) AS mau,
    SUM(wau) AS wau,
    SUM(dau) AS dau,
    SUM(visited_5_uri_mau) AS visited_5_uri_mau,
    SUM(visited_5_uri_wau) AS visited_5_uri_wau,
    SUM(visited_5_uri_dau) AS visited_5_uri_dau
  FROM
    `moz-fx-data-derived-datasets.telemetry.firefox_desktop_exact_mau28_by_dimensions_v1`
  GROUP BY
    submission_date ),
  -- We use imputed values for the period after the Armag-add-on deletion event;
  -- see https://bugzilla.mozilla.org/show_bug.cgi?id=1552558
  imputed AS (
  SELECT
    `date` AS submission_date,
    mau
  FROM
    static.firefox_desktop_imputed_mau28_v1
  WHERE
    datasource = 'desktop_global')
SELECT
  submission_date,
  coalesce(imputed.mau,
    base.mau) AS mau,
  base.* EXCEPT (submission_date,
    mau)
FROM
  base
LEFT JOIN
  imputed
USING
  (submission_date)
ORDER BY
  1
