CREATE
OR REPLACE VIEW `moz-fx-data-shared-prod`.telemetry_derived.experiment_enrollment_aggregates_live AS (
  WITH live AS (
    SELECT
      event_object AS `type`,
      event_string_value AS experiment,
      `moz-fx-data-shared-prod`.udf.get_key(event_map_values, 'branch') AS branch,
      TIMESTAMP_ADD(
        TIMESTAMP_TRUNC(`timestamp`, HOUR),
        INTERVAL (
          CAST(
            EXTRACT(
              MINUTE
              FROM
                `timestamp`
            ) / 5 AS int64
          ) * 5
        ) MINUTE
      ) AS window_start,
      TIMESTAMP_ADD(
        TIMESTAMP_TRUNC(`timestamp`, HOUR),
        INTERVAL (
          CAST(
            EXTRACT(
              MINUTE
              FROM
                `timestamp`
            ) / 5 + 1 AS int64
          ) * 5
        ) MINUTE
      ) AS window_end,
      COUNTIF(event_method = 'enroll') AS enroll_count,
      COUNTIF(event_method = 'unenroll') AS unenroll_count,
      COUNTIF(event_method = 'graduate') AS graduate_count,
      COUNTIF(event_method = 'update') AS update_count,
      COUNTIF(event_method = 'enrollFailed') AS enroll_failed_count,
      COUNTIF(event_method = 'unenrollFailed') AS unenroll_failed_count,
      COUNTIF(event_method = 'updateFailed') AS update_failed_count
    FROM
      `moz-fx-data-shared-prod`.telemetry_derived.events_live
    WHERE
      event_category = 'normandy'
      AND date(timestamp)
  > @submission_date
  GROUP BY
    1,
    2,
    3,
    4,
    5
),
previous AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod`.telemetry_derived.experiment_enrollment_aggregates_v1
  WHERE
    date(window_start) <= @submission_date
),
all_enrollments AS (
  SELECT
    *
  FROM
    previous
  UNION ALL
  SELECT
    *
  FROM
    live
)
SELECT
  *,
  SUM(enroll_count) OVER (
    PARTITION BY
      experiment,
      branch
    ORDER BY
      window_start
    ROWS BETWEEN
      UNBOUNDED PRECEDING
      AND CURRENT ROW
  ) AS cumulative_enroll_count,
  SUM(unenroll_count) OVER (
    PARTITION BY
      experiment,
      branch
    ORDER BY
      window_start
    ROWS BETWEEN
      UNBOUNDED PRECEDING
      AND CURRENT ROW
  ) AS cumulative_unenroll_count,
  SUM(graduate_count) OVER (
    PARTITION BY
      experiment,
      branch
    ORDER BY
      window_start
    ROWS BETWEEN
      UNBOUNDED PRECEDING
      AND CURRENT ROW
  ) AS cumulative_graduate_count,
  SUM(update_count) OVER (
    PARTITION BY
      experiment,
      branch
    ORDER BY
      window_start
    ROWS BETWEEN
      UNBOUNDED PRECEDING
      AND CURRENT ROW
  ) AS cumulative_update_count,
  SUM(enroll_failed_count) OVER (
    PARTITION BY
      experiment,
      branch
    ORDER BY
      window_start
  ) AS cumulative_enroll_failed_count,
  SUM(unenroll_failed_count) OVER (
    PARTITION BY
      experiment,
      branch
    ORDER BY
      window_start
  ) AS cumulative_unenroll_failed_count,
  SUM(update_failed_count) OVER (
    PARTITION BY
      experiment,
      branch
    ORDER BY
      window_start
  ) AS cumulative_update_failed_count
FROM
  all_enrollments
)
