SELECT
  submission_date,
  CURRENT_DATETIME() AS generated_time,
  product,
  SUM(mau) AS mau,
  SUM(wau) AS wau,
  SUM(dau) AS dau
FROM
  nondesktop_exact_mau28_by_dimensions_v1
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  product
