SELECT
  observation_date,
  last_submission_date_in_window,
  product,
  SUM(mau) AS mau,
  SUM(wau) AS wau,
  SUM(dau) AS dau,
  SUM(IF(country IN ('US', 'FR', 'DE', 'UK', 'CA'), mau, 0)) AS tier1_mau,
  SUM(IF(country IN ('US', 'FR', 'DE', 'UK', 'CA'), wau, 0)) AS tier1_wau,
  SUM(IF(country IN ('US', 'FR', 'DE', 'UK', 'CA'), dau, 0)) AS tier1_dau
FROM
  firefox_nondesktop_exact_mau28_by_dimensions_v1
GROUP BY
  observation_date,
  last_submission_date_in_window,
  product
