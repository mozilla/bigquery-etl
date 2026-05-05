CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.releases_latest`
AS
WITH channels AS (
  SELECT
    "release" AS channel
  UNION ALL
  SELECT
    "beta"
  UNION ALL
  SELECT
    "esr"
),
dates_and_channels AS (
  SELECT
    `date`,
    channel
  FROM
    UNNEST(
      (SELECT GENERATE_DATE_ARRAY("2000-01-01", CURRENT_DATE, INTERVAL 1 DAY) AS dates)
    ) AS `date`
  CROSS JOIN
    channels
),
releases AS (
  SELECT
    `date`,
    version,
    category,
    product,
    CASE
      WHEN category = "dev"
        THEN "beta"
      WHEN category = 'esr'
        THEN "esr"
      WHEN category IN ("stability", "major")
        THEN "release"
      ELSE "UNKNOWN"
    END AS channel,
    build_number,
  FROM
    `moz-fx-data-shared-prod.telemetry.releases`
),
joined AS (
  SELECT
    dates_and_channels.`date`,
    FIRST_VALUE(product IGNORE NULLS) OVER latest_values AS product,
    FIRST_VALUE(category IGNORE NULLS) OVER latest_values AS category,
    channel,
    FIRST_VALUE(build_number IGNORE NULLS) OVER latest_values AS build_number,
    FIRST_VALUE(releases.`date` IGNORE NULLS) OVER latest_values AS release_date,
    FIRST_VALUE(`version` IGNORE NULLS) OVER latest_values AS `version`,
  FROM
    dates_and_channels
  FULL OUTER JOIN
    releases
    USING (`date`, channel)
  WINDOW
    latest_values AS (
      PARTITION BY
        channel
      ORDER BY
        DATE DESC
      ROWS BETWEEN
        CURRENT ROW
        AND UNBOUNDED FOLLOWING
    )
)
SELECT
  *,
  mozfun.norm.extract_version(version, "major") AS major_version,
  mozfun.norm.extract_version(version, "minor") AS minor_version,
  mozfun.norm.extract_version(version, "patch") AS patch_version,
  mozfun.norm.extract_version(version, "beta") AS beta_version,
FROM
  joined
WHERE
  version IS NOT NULL
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      `date`,
      channel
    ORDER BY
      major_version,
      minor_version,
      patch_version DESC
  ) = 1
