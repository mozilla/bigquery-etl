SELECT
  submission_date,
  CURRENT_DATETIME() AS generated_time,
  SUM(mau) AS mau,
  SUM(dau) AS dau
FROM
  firefox_desktop_exact_mau28_by_dimensions_v1
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date
