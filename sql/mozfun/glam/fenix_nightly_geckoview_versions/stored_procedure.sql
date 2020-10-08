/*
Enumerate the most common GeckoView version for each build hour for "nightly"
Fenix builds.

This includes all hours up to the reference day e.g. the last row up where the
reference date is 2020-10-01 will include hours up to midnight of that day.

Example:

```
DECLARE results ARRAY<
    STRUCT<build_hour DATETIME, geckoview_version STRING, n_builds INT64>
>;
CALL fenix_nightly_geckoview_versions(DATE "2020-10-01", 28, results);
SELECT * FROM UNNEST(results)
```

The first row will be (2020-09-03T00:00:00, 82.0a1, 39901).
The last row will be (2020-10-01T00:00:00, 83.0a1, 46716).
*/
CREATE OR REPLACE PROCEDURE
  fenix_nightly_geckoview_versions(
    IN reference_date DATE,
    IN history_days INT64,
    OUT build_hour_version ARRAY<
      STRUCT<build_hour DATETIME, geckoview_version STRING, n_builds INT64>
    >
  )
BEGIN
  -- https://github.com/mozilla/bigquery-etl/blob/56defb1061445115277d90def06ad41185e519b5/sql/moz-fx-data-shared-prod/udf/fenix_build_to_datetime/metadata.yaml#L1-L23
  SET build_hour_version = (
    WITH extracted AS (
    -- We'll look at the metrics ping to estimate the major geckoview version
    -- the metrics section is aliased, so we must rename the table for this to
    -- work appropriately
      SELECT
        submission_timestamp,
        client_info.app_build,
        metrics.string.geckoview_version,
      FROM
        `moz-fx-data-shared-prod`.org_mozilla_fenix.metrics AS t1
      WHERE
        mozfun.norm.fenix_app_info('org_mozilla_fenix', client_info.app_build).channel = 'nightly'
      UNION ALL
      SELECT
        submission_timestamp,
        client_info.app_build,
        metrics.string.geckoview_version,
      FROM
        `moz-fx-data-shared-prod`.org_mozilla_fenix_nightly.metrics AS t1
      UNION ALL
      SELECT
        submission_timestamp,
        client_info.app_build,
        metrics.string.geckoview_version,
      FROM
        `moz-fx-data-shared-prod`.org_mozilla_fennec_aurora.metrics AS t1
    ),
    transformed AS (
      SELECT
        app_build,
        geckoview_version,
        datetime_TRUNC(
          `moz-fx-data-shared-prod`.udf.fenix_build_to_datetime(app_build),
          HOUR
        ) AS build_hour
      FROM
        extracted
      WHERE
        DATE(submission_timestamp) >= DATE_SUB(reference_date, INTERVAL history_days DAY)
    ),
    grouped_build_hours AS (-- count the number of geckoview versions for each build hour row over an interval
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
    top_build_hours AS (-- get the geckoview version for the build hour that has the most number of rows
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
    enumerated_build_hours AS (-- enumerate all of the build hours that we care about. We have a small margin that we'll
      -- use so we fill in null values for the rolling average of number of rows
      SELECT
        datetime(TIMESTAMP) AS build_hour
      FROM
        UNNEST(
          GENERATE_TIMESTAMP_ARRAY(
            TIMESTAMP_SUB(
              TIMESTAMP_TRUNC(CAST(reference_date AS timestamp), HOUR),
              INTERVAL history_days + 2 DAY
            ),
            TIMESTAMP_TRUNC(CAST(reference_date AS timestamp), HOUR),
            INTERVAL 1 HOUR
          )
        ) AS TIMESTAMP
    ),
    estimated_version AS (
      -- right join
      SELECT
        build_hour,
        MAX(geckoview_version) OVER (
          ORDER BY
            build_hour ASC
          ROWS BETWEEN
            UNBOUNDED PRECEDING
            AND CURRENT ROW
        ) AS geckoview_version,
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
    )
    SELECT
      ARRAY_AGG(
        STRUCT<build_hour DATETIME, geckoview_version STRING, n_builds INT64>(
          build_hour,
          geckoview_version,
          CAST(n_builds AS INT64)
        )
        ORDER BY
          build_hour
      )
    FROM
      estimated_version
    WHERE
      build_hour >= DATE_SUB(reference_date, INTERVAL history_days DAY)
  );
END;
