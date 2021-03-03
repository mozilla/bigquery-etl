SELECT
  submission_date,
  -- Cumulative days of use (CDOU) is a sum over the calendar year.
  SUM(dau) OVER (
    PARTITION BY
      EXTRACT(YEAR FROM submission_date),
      id_bucket,
      product,
      normalized_channel,
      campaign,
      country,
      distribution_id
    ORDER BY
      submission_date
    ROWS BETWEEN
      UNBOUNDED PRECEDING
      AND CURRENT ROW
  ) AS cdou,
  mau,
  wau,
  dau,
  id_bucket,
  product,
  normalized_channel,
  campaign,
  country,
  distribution_id
FROM
  firefox_nondesktop_exact_mau28_v1
WHERE
  -- We completely recreate this table every night since the source table
  -- is small and this query windows over a large time range.
  submission_date >= '2017-01-01'
