-- Query for telemetry health ping volume p80 across all applications
(
  WITH sample AS (
    SELECT
      "Firefox for Desktop" AS application,
      normalized_channel AS channel,
      DATE(submission_timestamp) AS submission_date,
      client_info.client_id,
      COUNT(1) AS ping_count
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_stable.baseline_v1`
    WHERE
      sample_id = 0
      AND DATE(submission_timestamp) = @submission_date
    GROUP BY
      application,
      channel,
      submission_date,
      client_info.client_id
  ),
  ping_count_quantiles AS (
    SELECT
      application,
      channel,
      submission_date,
      APPROX_QUANTILES(ping_count, 100) AS quantiles,
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
    quantiles[OFFSET(80)] AS p80
  FROM
    ping_count_quantiles
)
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for Android" AS application,
        mozfun.norm.fenix_app_info("org_mozilla_firefox", client_info.app_build).channel AS channel,
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id,
        COUNT(1) AS ping_count
      FROM
        `moz-fx-data-shared-prod.org_mozilla_firefox_stable.baseline_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        application,
        channel,
        submission_date,
        client_info.client_id
    ),
    ping_count_quantiles AS (
      SELECT
        application,
        channel,
        submission_date,
        APPROX_QUANTILES(ping_count, 100) AS quantiles,
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
      quantiles[OFFSET(80)] AS p80
    FROM
      ping_count_quantiles
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
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id,
        COUNT(1) AS ping_count
      FROM
        `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.baseline_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        application,
        channel,
        submission_date,
        client_info.client_id
    ),
    ping_count_quantiles AS (
      SELECT
        application,
        channel,
        submission_date,
        APPROX_QUANTILES(ping_count, 100) AS quantiles,
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
      quantiles[OFFSET(80)] AS p80
    FROM
      ping_count_quantiles
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for Android" AS application,
        mozfun.norm.fenix_app_info("org_mozilla_fenix", client_info.app_build).channel AS channel,
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id,
        COUNT(1) AS ping_count
      FROM
        `moz-fx-data-shared-prod.org_mozilla_fenix_stable.baseline_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        application,
        channel,
        submission_date,
        client_info.client_id
    ),
    ping_count_quantiles AS (
      SELECT
        application,
        channel,
        submission_date,
        APPROX_QUANTILES(ping_count, 100) AS quantiles,
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
      quantiles[OFFSET(80)] AS p80
    FROM
      ping_count_quantiles
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for iOS" AS application,
        normalized_channel AS channel,
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id,
        COUNT(1) AS ping_count
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.baseline_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        application,
        channel,
        submission_date,
        client_info.client_id
    ),
    ping_count_quantiles AS (
      SELECT
        application,
        channel,
        submission_date,
        APPROX_QUANTILES(ping_count, 100) AS quantiles,
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
      quantiles[OFFSET(80)] AS p80
    FROM
      ping_count_quantiles
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for iOS" AS application,
        normalized_channel AS channel,
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id,
        COUNT(1) AS ping_count
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.baseline_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        application,
        channel,
        submission_date,
        client_info.client_id
    ),
    ping_count_quantiles AS (
      SELECT
        application,
        channel,
        submission_date,
        APPROX_QUANTILES(ping_count, 100) AS quantiles,
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
      quantiles[OFFSET(80)] AS p80
    FROM
      ping_count_quantiles
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for iOS" AS application,
        normalized_channel AS channel,
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id,
        COUNT(1) AS ping_count
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.baseline_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
      GROUP BY
        application,
        channel,
        submission_date,
        client_info.client_id
    ),
    ping_count_quantiles AS (
      SELECT
        application,
        channel,
        submission_date,
        APPROX_QUANTILES(ping_count, 100) AS quantiles,
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
      quantiles[OFFSET(80)] AS p80
    FROM
      ping_count_quantiles
  )
