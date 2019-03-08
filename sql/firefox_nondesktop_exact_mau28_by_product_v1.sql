SELECT
  submission_date,
  observation_date,
  CURRENT_DATETIME() AS generated_time,
  product,
  SUM(mau) AS mau,
  SUM(wau) AS wau,
  SUM(dau) AS dau,
  SUM(IF(country IN ('US', 'FR', 'DE', 'UK', 'CA'), mau, 0)) AS tier1_mau,
  SUM(IF(country IN ('US', 'FR', 'DE', 'UK', 'CA'), wau, 0)) AS tier1_wau,
  SUM(IF(country IN ('US', 'FR', 'DE', 'UK', 'CA'), dau, 0)) AS tier1_dau
FROM
  firefox_nondesktop_exact_mau28_by_product_by_dimensions_v1
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  observation_date,
  product
