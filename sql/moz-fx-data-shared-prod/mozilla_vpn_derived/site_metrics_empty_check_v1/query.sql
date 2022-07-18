SELECT
  IF(COUNT(*) = 0, ERROR(CONCAT('No data for mozilla.org on ', @date)), NULL)
FROM
  `moz-fx-data-marketing-prod.65789850.ga_sessions_*`
WHERE
  _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @date)
UNION ALL
SELECT
  -- We stopped getting Google Analytics data for vpn.mozilla.org after 2022-06-21.
  IF(
    (COUNT(*) = 0 AND @date <= '2022-06-21'),
    ERROR(CONCAT('No data for vpn.mozilla.org on ', @date)),
    NULL
  )
FROM
  `moz-fx-data-marketing-prod.220432379.ga_sessions_*`
WHERE
  _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @date)
