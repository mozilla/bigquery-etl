-- Query for telemetry health sequence holes across all applications
(
  WITH sample AS (
    SELECT
      "Firefox for Desktop" AS application,
      normalized_channel AS channel,
      DATE(submission_timestamp) AS submission_date,
      client_info.client_id,
      ping_info.seq AS sequence_number
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_stable.baseline_v1`
    WHERE
      sample_id = 0
      AND DATE(submission_timestamp) = @submission_date
  ),
  lagged AS (
    SELECT
      application,
      channel,
      submission_date,
      client_id,
      sequence_number,
      LAG(sequence_number) OVER (PARTITION BY client_id ORDER BY sequence_number) AS prev_seq
    FROM
      sample
  ),
  per_client_day AS (
    SELECT
      application,
      channel,
      submission_date,
      client_id,
      -- A client has a gap on that date if any step isn't prev+1.
      LOGICAL_OR(prev_seq IS NOT NULL AND sequence_number != prev_seq + 1) AS has_gap
    FROM
      lagged
    GROUP BY
      application,
      channel,
      submission_date,
      client_id
  )
  SELECT
    application,
    channel,
    submission_date,
    COUNTIF(has_gap) AS clients_with_sequence_gaps_1pct,
    COUNT(DISTINCT client_id) AS total_unique_clients_1pct,
    SAFE_DIVIDE(COUNTIF(has_gap), COUNT(DISTINCT client_id)) * 100 AS pct_clients_with_gaps
  FROM
    per_client_day
  GROUP BY
    application,
    channel,
    submission_date
)
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for Android" AS application,
        mozfun.norm.fenix_app_info("org_mozilla_firefox", client_info.app_build).channel AS channel,
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id,
        ping_info.seq AS sequence_number
      FROM
        `moz-fx-data-shared-prod.org_mozilla_firefox_stable.baseline_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
    ),
    lagged AS (
      SELECT
        application,
        channel,
        submission_date,
        client_id,
        sequence_number,
        LAG(sequence_number) OVER (PARTITION BY client_id ORDER BY sequence_number) AS prev_seq
      FROM
        sample
    ),
    per_client_day AS (
      SELECT
        application,
        channel,
        submission_date,
        client_id,
      -- A client has a gap on that date if any step isn't prev+1.
        LOGICAL_OR(prev_seq IS NOT NULL AND sequence_number != prev_seq + 1) AS has_gap
      FROM
        lagged
      GROUP BY
        application,
        channel,
        submission_date,
        client_id
    )
    SELECT
      application,
      channel,
      submission_date,
      COUNTIF(has_gap) AS clients_with_sequence_gaps_1pct,
      COUNT(DISTINCT client_id) AS total_unique_clients_1pct,
      SAFE_DIVIDE(COUNTIF(has_gap), COUNT(DISTINCT client_id)) * 100 AS pct_clients_with_gaps
    FROM
      per_client_day
    GROUP BY
      application,
      channel,
      submission_date
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
        ping_info.seq AS sequence_number
      FROM
        `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.baseline_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
    ),
    lagged AS (
      SELECT
        application,
        channel,
        submission_date,
        client_id,
        sequence_number,
        LAG(sequence_number) OVER (PARTITION BY client_id ORDER BY sequence_number) AS prev_seq
      FROM
        sample
    ),
    per_client_day AS (
      SELECT
        application,
        channel,
        submission_date,
        client_id,
      -- A client has a gap on that date if any step isn't prev+1.
        LOGICAL_OR(prev_seq IS NOT NULL AND sequence_number != prev_seq + 1) AS has_gap
      FROM
        lagged
      GROUP BY
        application,
        channel,
        submission_date,
        client_id
    )
    SELECT
      application,
      channel,
      submission_date,
      COUNTIF(has_gap) AS clients_with_sequence_gaps_1pct,
      COUNT(DISTINCT client_id) AS total_unique_clients_1pct,
      SAFE_DIVIDE(COUNTIF(has_gap), COUNT(DISTINCT client_id)) * 100 AS pct_clients_with_gaps
    FROM
      per_client_day
    GROUP BY
      application,
      channel,
      submission_date
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for Android" AS application,
        mozfun.norm.fenix_app_info("org_mozilla_fenix", client_info.app_build).channel AS channel,
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id,
        ping_info.seq AS sequence_number
      FROM
        `moz-fx-data-shared-prod.org_mozilla_fenix_stable.baseline_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
			-- There's still some older builds under the `org_mozilla_fenix` app_id reporting
			-- channels other than "nightly", so let's ignore the really old builds.
        AND SAFE_CAST(client_info.app_build AS INT64) >= 21850000
    ),
    lagged AS (
      SELECT
        application,
        channel,
        submission_date,
        client_id,
        sequence_number,
        LAG(sequence_number) OVER (PARTITION BY client_id ORDER BY sequence_number) AS prev_seq
      FROM
        sample
    ),
    per_client_day AS (
      SELECT
        application,
        channel,
        submission_date,
        client_id,
      -- A client has a gap on that date if any step isn't prev+1.
        LOGICAL_OR(prev_seq IS NOT NULL AND sequence_number != prev_seq + 1) AS has_gap
      FROM
        lagged
      GROUP BY
        application,
        channel,
        submission_date,
        client_id
    )
    SELECT
      application,
      channel,
      submission_date,
      COUNTIF(has_gap) AS clients_with_sequence_gaps_1pct,
      COUNT(DISTINCT client_id) AS total_unique_clients_1pct,
      SAFE_DIVIDE(COUNTIF(has_gap), COUNT(DISTINCT client_id)) * 100 AS pct_clients_with_gaps
    FROM
      per_client_day
    GROUP BY
      application,
      channel,
      submission_date
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for iOS" AS application,
        normalized_channel AS channel,
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id,
        ping_info.seq AS sequence_number
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.baseline_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
    ),
    lagged AS (
      SELECT
        application,
        channel,
        submission_date,
        client_id,
        sequence_number,
        LAG(sequence_number) OVER (PARTITION BY client_id ORDER BY sequence_number) AS prev_seq
      FROM
        sample
    ),
    per_client_day AS (
      SELECT
        application,
        channel,
        submission_date,
        client_id,
      -- A client has a gap on that date if any step isn't prev+1.
        LOGICAL_OR(prev_seq IS NOT NULL AND sequence_number != prev_seq + 1) AS has_gap
      FROM
        lagged
      GROUP BY
        application,
        channel,
        submission_date,
        client_id
    )
    SELECT
      application,
      channel,
      submission_date,
      COUNTIF(has_gap) AS clients_with_sequence_gaps_1pct,
      COUNT(DISTINCT client_id) AS total_unique_clients_1pct,
      SAFE_DIVIDE(COUNTIF(has_gap), COUNT(DISTINCT client_id)) * 100 AS pct_clients_with_gaps
    FROM
      per_client_day
    GROUP BY
      application,
      channel,
      submission_date
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for iOS" AS application,
        normalized_channel AS channel,
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id,
        ping_info.seq AS sequence_number
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.baseline_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
    ),
    lagged AS (
      SELECT
        application,
        channel,
        submission_date,
        client_id,
        sequence_number,
        LAG(sequence_number) OVER (PARTITION BY client_id ORDER BY sequence_number) AS prev_seq
      FROM
        sample
    ),
    per_client_day AS (
      SELECT
        application,
        channel,
        submission_date,
        client_id,
      -- A client has a gap on that date if any step isn't prev+1.
        LOGICAL_OR(prev_seq IS NOT NULL AND sequence_number != prev_seq + 1) AS has_gap
      FROM
        lagged
      GROUP BY
        application,
        channel,
        submission_date,
        client_id
    )
    SELECT
      application,
      channel,
      submission_date,
      COUNTIF(has_gap) AS clients_with_sequence_gaps_1pct,
      COUNT(DISTINCT client_id) AS total_unique_clients_1pct,
      SAFE_DIVIDE(COUNTIF(has_gap), COUNT(DISTINCT client_id)) * 100 AS pct_clients_with_gaps
    FROM
      per_client_day
    GROUP BY
      application,
      channel,
      submission_date
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for iOS" AS application,
        normalized_channel AS channel,
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id,
        ping_info.seq AS sequence_number
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.baseline_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
    ),
    lagged AS (
      SELECT
        application,
        channel,
        submission_date,
        client_id,
        sequence_number,
        LAG(sequence_number) OVER (PARTITION BY client_id ORDER BY sequence_number) AS prev_seq
      FROM
        sample
    ),
    per_client_day AS (
      SELECT
        application,
        channel,
        submission_date,
        client_id,
      -- A client has a gap on that date if any step isn't prev+1.
        LOGICAL_OR(prev_seq IS NOT NULL AND sequence_number != prev_seq + 1) AS has_gap
      FROM
        lagged
      GROUP BY
        application,
        channel,
        submission_date,
        client_id
    )
    SELECT
      application,
      channel,
      submission_date,
      COUNTIF(has_gap) AS clients_with_sequence_gaps_1pct,
      COUNT(DISTINCT client_id) AS total_unique_clients_1pct,
      SAFE_DIVIDE(COUNTIF(has_gap), COUNT(DISTINCT client_id)) * 100 AS pct_clients_with_gaps
    FROM
      per_client_day
    GROUP BY
      application,
      channel,
      submission_date
  )
