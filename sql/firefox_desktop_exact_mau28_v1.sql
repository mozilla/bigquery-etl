CREATE OR REPLACE VIEW
  firefox_desktop_exact_mau28_v1 AS
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
  submission_date
