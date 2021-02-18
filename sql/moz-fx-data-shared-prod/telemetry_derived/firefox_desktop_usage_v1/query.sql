SELECT
  submission_date,
  -- Cumulative days of use (CDOU) is a sum over the calendar year.
  SUM(dau) OVER (
    PARTITION BY
      EXTRACT(YEAR FROM submission_date),
      id_bucket,
      activity_segment,
      os,
      `source`,
      medium,
      campaign,
      content,
      country,
      distribution_id
  ) AS cdou,
  mau,
  wau,
  dau,
  new_profiles,
  id_bucket,
  activity_segment,
  os,
  `source`,
  medium,
  campaign,
  content,
  country,
  distribution_id
FROM
  firefox_desktop_exact_mau28_by_dimensions_v2
WHERE
  -- We completely recreate this table every night since the source table
  -- is small and this query windows over a large time range.
  submission_date >= '2017-01-01'
