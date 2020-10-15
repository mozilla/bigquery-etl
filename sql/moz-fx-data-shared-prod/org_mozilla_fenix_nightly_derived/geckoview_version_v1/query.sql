WITH extracted AS (
    -- We'll look at the metrics ping to estimate the major geckoview version.
    -- The metrics section is aliased, so we must rename the table for this to
    -- work as expected.
  SELECT
    submission_timestamp,
    client_info.app_build,
    metrics.string.geckoview_version,
  FROM
    org_mozilla_fenix.metrics AS t1
  WHERE
    mozfun.norm.fenix_app_info('org_mozilla_fenix', client_info.app_build).channel = 'nightly'
  UNION ALL
  SELECT
    submission_timestamp,
    client_info.app_build,
    metrics.string.geckoview_version,
  FROM
    org_mozilla_fenix_nightly.metrics AS t1
  UNION ALL
  SELECT
    submission_timestamp,
    client_info.app_build,
    metrics.string.geckoview_version,
  FROM
    org_mozilla_fennec_aurora.metrics AS t1
),
transformed AS (
  SELECT
    app_build,
    geckoview_version,
        -- Truncate to the hour, since older builds give minute resolution.
    datetime_TRUNC(
      `moz-fx-data-shared-prod.udf.fenix_build_to_datetime`(app_build),
      HOUR
    ) AS build_hour
  FROM
    extracted
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_SUB(@submission_date, INTERVAL 30 DAY)
    AND @submission_date
),
grouped_build_hours AS (
      -- Count the number of geckoview versions for each build hour row over the
      -- expected interval. We choose a minimum number of builds to filter out
      -- noise.
  SELECT
    build_hour,
    geckoview_version,
    COUNT(*) AS n_builds
  FROM
    transformed
  WHERE
    geckoview_version IS NOT NULL
    AND app_build IS NOT NULL
    AND build_hour IS NOT NULL
  GROUP BY
    build_hour,
    geckoview_version
  HAVING
    n_builds > 5
  ORDER BY
    build_hour DESC,
    geckoview_version
),
top_build_hours AS (
      -- Get the geckoview version for the build hour that has the most number
      -- of rows.
  SELECT
    ROW.*
  FROM
    (
      SELECT
        ARRAY_AGG(t ORDER BY t.n_builds DESC LIMIT 1)[OFFSET(0)] ROW
      FROM
        grouped_build_hours t
      GROUP BY
        build_hour
    )
),
enumerated_build_hours AS (
    -- Enumerate all of the build hours that we care about. We have a small
    -- margin that we'll use so we fill in null values for the rolling average
    -- of number of rows.
  SELECT
    datetime(`timestamp`) AS build_hour
  FROM
    UNNEST(
      GENERATE_TIMESTAMP_ARRAY(
        TIMESTAMP_SUB(
          TIMESTAMP_TRUNC(CAST(@submission_date AS timestamp), HOUR),
          INTERVAL 30 + 2 DAY
        ),
        TIMESTAMP_TRUNC(CAST(@submission_date AS timestamp), HOUR),
        INTERVAL 1 HOUR
      )
    ) AS `timestamp`
),
estimated_version AS (
  SELECT
    build_hour,
        -- Versions are expected to be monotonically increasing.
    MAX(geckoview_version) OVER (
      ORDER BY
        build_hour ASC
      ROWS BETWEEN
        UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS geckoview_version,
        -- The number of builds is used as a query diagnostic.
    AVG(n_builds) OVER (
      ORDER BY
        build_hour ASC
      ROWS BETWEEN
        48 PRECEDING
        AND CURRENT ROW
    ) AS n_builds
  FROM
    top_build_hours
  RIGHT JOIN
    enumerated_build_hours
  USING
    (build_hour)
),
new_geckoview_versions AS (
  SELECT
    build_hour,
    geckoview_version,
    CAST(n_builds AS INT64) AS n_builds
  FROM
    estimated_version
  WHERE
    build_hour
    BETWEEN DATE_SUB(@submission_date, INTERVAL 30 DAY)
    AND @submission_date
  ORDER BY
    build_hour
),
past_and_present AS (
  SELECT
    *
  FROM
    new_geckoview_versions
  UNION ALL
  SELECT
    *
  FROM
    org_mozilla_fenix_nightly_derived.geckoview_version_v1
)
-- take the average over the previous values
SELECT
  ROW.*
FROM
  (
    SELECT
      ARRAY_AGG(t ORDER BY t.n_builds DESC LIMIT 1)[OFFSET(0)] ROW
    FROM
      past_and_present t
    GROUP BY
      build_hour
  )
ORDER BY
  build_hour DESC
