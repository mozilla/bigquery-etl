WITH channels AS (
  SELECT
  "release" AS channel
  UNION ALL SELECT "beta"
),
dates_and_channels AS (
  SELECT
    `date`, channel
  FROM
    UNNEST(
      (SELECT GENERATE_DATE_ARRAY(DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR), CURRENT_DATE, INTERVAL 1 DAY) AS dates)
    ) AS `date`
    CROSS JOIN channels
),
builds AS (
  SELECT
    submission_timestamp,
    build.target.channel AS channel,
    build.target.`version`,
    build.build.`date` AS build_date,
  FROM `moz-fx-data-shared-prod.telemetry.buildhub2`
  WHERE build.source.product = "firefox"
  QUALIFY
    ROW_NUMBER() OVER(PARTITION BY channel, version ORDER BY submission_timestamp DESC) = 1
),
releases AS (
  SELECT
    `date`,
    `version`,
    category,
    product,
    build_date,
    COALESCE(
      channel,
      CASE
          WHEN category NOT IN ('esr', 'dev') THEN "release"
          WHEN category = 'esr' THEN "esr"
          WHEN category = 'dev' THEN 'beta'
      END
    ) AS channel,
  FROM telemetry.releases
  LEFT JOIN builds using(`version`)
  -- we need to dedup per date, channel due to some cases where
  -- we get some versions marked as the wrong channel.
  QUALIFY
    ROW_NUMBER() OVER(PARTITION BY `date`, channel ORDER BY submission_timestamp DESC) = 1
),
joined AS (
  SELECT
  dates_and_channels.`date`,
  FIRST_VALUE(product IGNORE NULLS) OVER latest_values AS product,
  FIRST_VALUE(category IGNORE NULLS) OVER latest_values AS category,
  channel,
  DATE(FIRST_VALUE(build_date IGNORE NULLS) OVER latest_values) AS build_date,
  FIRST_VALUE(releases.`date` IGNORE NULLS) OVER latest_values AS release_date,
  FIRST_VALUE(`version` IGNORE NULLS) OVER latest_values AS `version`,
  FROM dates_and_channels
  FULL OUTER JOIN releases USING(`date`, channel)
  WHERE channel NOT IN ("aurora", "esr")
  WINDOW latest_values AS (
    PARTITION BY channel ORDER BY DATE DESC
    ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
  )
)
SELECT
  *,
  mozfun.norm.extract_version(version, "major") AS major_version,
  mozfun.norm.extract_version(version, "minor") AS minor_version,
  mozfun.norm.extract_version(version, "patch") AS patch_version,
FROM joined
