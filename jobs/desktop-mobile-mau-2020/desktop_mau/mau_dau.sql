WITH mau_dau AS (
  SELECT
    submission_date AS date,
    country,
    SUM(mau) AS MAU,
    SUM(dau) AS DAU,
  FROM
    `moz-fx-data-shared-prod.telemetry.firefox_desktop_exact_mau28_by_dimensions`
  WHERE
    country IN ("US", "CA", "DE", "FR", "GB", "CN", "IN", "BR", "ID", "RU", "PL")
    AND submission_date >= "2016-12-01"
  GROUP BY
    date,
    country
  UNION ALL
  SELECT
    submission_date AS date,
    "Global" AS country,
    SUM(mau) AS MAU,
    SUM(dau) AS DAU
  FROM
    `moz-fx-data-shared-prod.telemetry.firefox_desktop_exact_mau28_by_dimensions`
  WHERE
    submission_date >= "2016-12-01"
  GROUP BY
    date,
    country
  UNION ALL
  SELECT
    submission_date AS date,
    CASE
    WHEN
      country IN ("US", "CA", "DE", "FR", "GB")
    THEN
      "Tier1"
    ELSE
      "RoW"
    END
    AS country,
    SUM(mau) AS MAU,
    SUM(dau) AS DAU
  FROM
    `moz-fx-data-shared-prod.telemetry.firefox_desktop_exact_mau28_by_dimensions`
  WHERE
    submission_date >= "2016-12-01"
  GROUP BY
    date,
    country
)
SELECT
  *
FROM
  mau_dau
ORDER BY
  date,
  country
