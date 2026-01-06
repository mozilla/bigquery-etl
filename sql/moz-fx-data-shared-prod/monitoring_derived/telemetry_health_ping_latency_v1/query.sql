-- Query for telemetry health ping latency across all applications
(
  WITH sample AS (
    SELECT
      "Firefox for Desktop" AS application,
      normalized_channel AS channel,
      metadata.header.parsed_date,
      ping_info.parsed_end_time,
      submission_timestamp,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop.baseline`
    WHERE
      sample_id = 0
      AND DATE(submission_timestamp) = @submission_date
  ),
  latency_quantiles AS (
    SELECT
      application,
      channel,
      DATE(submission_timestamp) AS submission_date,
      APPROX_QUANTILES(
        TIMESTAMP_DIFF(parsed_date, parsed_end_time, SECOND),
        100
      ) AS collection_to_submission_latency,
      APPROX_QUANTILES(
        TIMESTAMP_DIFF(submission_timestamp, parsed_date, SECOND),
        100
      ) AS submission_to_ingestion_latency,
      APPROX_QUANTILES(
        TIMESTAMP_DIFF(submission_timestamp, parsed_end_time, SECOND),
        100
      ) AS collection_to_ingestion_latency
    FROM
      sample
    GROUP BY
      application,
      channel,
      submission_date
  )
  SELECT
    application,
    channel,
    submission_date,
    collection_to_submission_latency[OFFSET(95)] AS collection_to_submission_latency_p95,
    collection_to_submission_latency[OFFSET(50)] AS collection_to_submission_latency_median,
    submission_to_ingestion_latency[OFFSET(95)] AS submission_to_ingestion_latency_p95,
    submission_to_ingestion_latency[OFFSET(50)] AS submission_to_ingestion_latency_median,
    collection_to_ingestion_latency[OFFSET(95)] AS collection_to_ingestion_latency_p95,
    collection_to_ingestion_latency[OFFSET(50)] AS collection_to_ingestion_latency_median
  FROM
    latency_quantiles
)
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for Android" AS application,
        mozfun.norm.fenix_app_info("org_mozilla_firefox", client_info.app_build).channel AS channel,
        metadata.header.parsed_date,
        ping_info.parsed_end_time,
        submission_timestamp,
      FROM
        `moz-fx-data-shared-prod.org_mozilla_firefox.baseline`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
    ),
    latency_quantiles AS (
      SELECT
        application,
        channel,
        DATE(submission_timestamp) AS submission_date,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(parsed_date, parsed_end_time, SECOND),
          100
        ) AS collection_to_submission_latency,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(submission_timestamp, parsed_date, SECOND),
          100
        ) AS submission_to_ingestion_latency,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(submission_timestamp, parsed_end_time, SECOND),
          100
        ) AS collection_to_ingestion_latency
      FROM
        sample
      GROUP BY
        application,
        channel,
        submission_date
    )
    SELECT
      application,
      channel,
      submission_date,
      collection_to_submission_latency[OFFSET(95)] AS collection_to_submission_latency_p95,
      collection_to_submission_latency[OFFSET(50)] AS collection_to_submission_latency_median,
      submission_to_ingestion_latency[OFFSET(95)] AS submission_to_ingestion_latency_p95,
      submission_to_ingestion_latency[OFFSET(50)] AS submission_to_ingestion_latency_median,
      collection_to_ingestion_latency[OFFSET(95)] AS collection_to_ingestion_latency_p95,
      collection_to_ingestion_latency[OFFSET(50)] AS collection_to_ingestion_latency_median
    FROM
      latency_quantiles
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for Android" AS application,
        mozfun.norm.fenix_app_info(
          "org_mozilla_firefox_beta",
          client_info.app_build
        ).channel AS channel,
        metadata.header.parsed_date,
        ping_info.parsed_end_time,
        submission_timestamp,
      FROM
        `moz-fx-data-shared-prod.org_mozilla_firefox_beta.baseline`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
    ),
    latency_quantiles AS (
      SELECT
        application,
        channel,
        DATE(submission_timestamp) AS submission_date,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(parsed_date, parsed_end_time, SECOND),
          100
        ) AS collection_to_submission_latency,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(submission_timestamp, parsed_date, SECOND),
          100
        ) AS submission_to_ingestion_latency,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(submission_timestamp, parsed_end_time, SECOND),
          100
        ) AS collection_to_ingestion_latency
      FROM
        sample
      GROUP BY
        application,
        channel,
        submission_date
    )
    SELECT
      application,
      channel,
      submission_date,
      collection_to_submission_latency[OFFSET(95)] AS collection_to_submission_latency_p95,
      collection_to_submission_latency[OFFSET(50)] AS collection_to_submission_latency_median,
      submission_to_ingestion_latency[OFFSET(95)] AS submission_to_ingestion_latency_p95,
      submission_to_ingestion_latency[OFFSET(50)] AS submission_to_ingestion_latency_median,
      collection_to_ingestion_latency[OFFSET(95)] AS collection_to_ingestion_latency_p95,
      collection_to_ingestion_latency[OFFSET(50)] AS collection_to_ingestion_latency_median
    FROM
      latency_quantiles
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for Android" AS application,
        mozfun.norm.fenix_app_info("org_mozilla_fenix", client_info.app_build).channel AS channel,
        metadata.header.parsed_date,
        ping_info.parsed_end_time,
        submission_timestamp,
      FROM
        `moz-fx-data-shared-prod.org_mozilla_fenix.baseline`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
			-- There's still some older builds under the `org_mozilla_fenix` app_id reporting
			-- channels other than "nightly", so let's ignore the really old builds.
        AND SAFE_CAST(client_info.app_build AS INT64) >= 21850000
    ),
    latency_quantiles AS (
      SELECT
        application,
        channel,
        DATE(submission_timestamp) AS submission_date,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(parsed_date, parsed_end_time, SECOND),
          100
        ) AS collection_to_submission_latency,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(submission_timestamp, parsed_date, SECOND),
          100
        ) AS submission_to_ingestion_latency,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(submission_timestamp, parsed_end_time, SECOND),
          100
        ) AS collection_to_ingestion_latency
      FROM
        sample
      GROUP BY
        application,
        channel,
        submission_date
    )
    SELECT
      application,
      channel,
      submission_date,
      collection_to_submission_latency[OFFSET(95)] AS collection_to_submission_latency_p95,
      collection_to_submission_latency[OFFSET(50)] AS collection_to_submission_latency_median,
      submission_to_ingestion_latency[OFFSET(95)] AS submission_to_ingestion_latency_p95,
      submission_to_ingestion_latency[OFFSET(50)] AS submission_to_ingestion_latency_median,
      collection_to_ingestion_latency[OFFSET(95)] AS collection_to_ingestion_latency_p95,
      collection_to_ingestion_latency[OFFSET(50)] AS collection_to_ingestion_latency_median
    FROM
      latency_quantiles
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for iOS" AS application,
        normalized_channel AS channel,
        metadata.header.parsed_date,
        ping_info.parsed_end_time,
        submission_timestamp,
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_firefox.baseline`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
    ),
    latency_quantiles AS (
      SELECT
        application,
        channel,
        DATE(submission_timestamp) AS submission_date,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(parsed_date, parsed_end_time, SECOND),
          100
        ) AS collection_to_submission_latency,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(submission_timestamp, parsed_date, SECOND),
          100
        ) AS submission_to_ingestion_latency,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(submission_timestamp, parsed_end_time, SECOND),
          100
        ) AS collection_to_ingestion_latency
      FROM
        sample
      GROUP BY
        application,
        channel,
        submission_date
    )
    SELECT
      application,
      channel,
      submission_date,
      collection_to_submission_latency[OFFSET(95)] AS collection_to_submission_latency_p95,
      collection_to_submission_latency[OFFSET(50)] AS collection_to_submission_latency_median,
      submission_to_ingestion_latency[OFFSET(95)] AS submission_to_ingestion_latency_p95,
      submission_to_ingestion_latency[OFFSET(50)] AS submission_to_ingestion_latency_median,
      collection_to_ingestion_latency[OFFSET(95)] AS collection_to_ingestion_latency_p95,
      collection_to_ingestion_latency[OFFSET(50)] AS collection_to_ingestion_latency_median
    FROM
      latency_quantiles
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for iOS" AS application,
        normalized_channel AS channel,
        metadata.header.parsed_date,
        ping_info.parsed_end_time,
        submission_timestamp,
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta.baseline`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
    ),
    latency_quantiles AS (
      SELECT
        application,
        channel,
        DATE(submission_timestamp) AS submission_date,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(parsed_date, parsed_end_time, SECOND),
          100
        ) AS collection_to_submission_latency,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(submission_timestamp, parsed_date, SECOND),
          100
        ) AS submission_to_ingestion_latency,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(submission_timestamp, parsed_end_time, SECOND),
          100
        ) AS collection_to_ingestion_latency
      FROM
        sample
      GROUP BY
        application,
        channel,
        submission_date
    )
    SELECT
      application,
      channel,
      submission_date,
      collection_to_submission_latency[OFFSET(95)] AS collection_to_submission_latency_p95,
      collection_to_submission_latency[OFFSET(50)] AS collection_to_submission_latency_median,
      submission_to_ingestion_latency[OFFSET(95)] AS submission_to_ingestion_latency_p95,
      submission_to_ingestion_latency[OFFSET(50)] AS submission_to_ingestion_latency_median,
      collection_to_ingestion_latency[OFFSET(95)] AS collection_to_ingestion_latency_p95,
      collection_to_ingestion_latency[OFFSET(50)] AS collection_to_ingestion_latency_median
    FROM
      latency_quantiles
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for iOS" AS application,
        normalized_channel AS channel,
        metadata.header.parsed_date,
        ping_info.parsed_end_time,
        submission_timestamp,
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_fennec.baseline`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
    ),
    latency_quantiles AS (
      SELECT
        application,
        channel,
        DATE(submission_timestamp) AS submission_date,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(parsed_date, parsed_end_time, SECOND),
          100
        ) AS collection_to_submission_latency,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(submission_timestamp, parsed_date, SECOND),
          100
        ) AS submission_to_ingestion_latency,
        APPROX_QUANTILES(
          TIMESTAMP_DIFF(submission_timestamp, parsed_end_time, SECOND),
          100
        ) AS collection_to_ingestion_latency
      FROM
        sample
      GROUP BY
        application,
        channel,
        submission_date
    )
    SELECT
      application,
      channel,
      submission_date,
      collection_to_submission_latency[OFFSET(95)] AS collection_to_submission_latency_p95,
      collection_to_submission_latency[OFFSET(50)] AS collection_to_submission_latency_median,
      submission_to_ingestion_latency[OFFSET(95)] AS submission_to_ingestion_latency_p95,
      submission_to_ingestion_latency[OFFSET(50)] AS submission_to_ingestion_latency_median,
      collection_to_ingestion_latency[OFFSET(95)] AS collection_to_ingestion_latency_p95,
      collection_to_ingestion_latency[OFFSET(50)] AS collection_to_ingestion_latency_median
    FROM
      latency_quantiles
  )
