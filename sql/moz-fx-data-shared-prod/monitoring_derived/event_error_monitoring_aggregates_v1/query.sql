-- Generated via ./bqetl generate glean_usage
-- This table aggregates event collection errors across Glean applications.
-- For details on the error types, see
-- https://mozilla.github.io/glean/book/reference/metrics/event.html#recorded-errors
(
  WITH event_counters AS (
    SELECT
      DATE(submission_timestamp) AS submission_date,
      "Firefox for Desktop" AS normalized_app_name,
      client_info.app_channel AS channel,
      metrics.labeled_counter
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_stable.events_v1`
    WHERE
      DATE(submission_timestamp) = @submission_date
  )
  SELECT
    submission_date,
    normalized_app_name,
    channel,
    'overflow' AS error_type,
    KEY AS metric,
    COALESCE(SUM(value), 0) AS error_sum
  FROM
    event_counters,
    UNNEST(labeled_counter.glean_error_invalid_overflow)
  GROUP BY
    submission_date,
    normalized_app_name,
    channel,
    error_type,
    metric
  UNION ALL
  SELECT
    submission_date,
    normalized_app_name,
    channel,
    'invalid_value' AS error_type,
    KEY AS metric,
    COALESCE(SUM(value), 0) AS error_sum
  FROM
    event_counters,
    UNNEST(labeled_counter.glean_error_invalid_value)
  GROUP BY
    submission_date,
    normalized_app_name,
    channel,
    error_type,
    metric
)
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox for Desktop Background Update Task" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.firefox_desktop_background_update_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox Desktop Default Agent Task" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.firefox_desktop_background_defaultagent_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Pinebuild" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.pine_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox for Android" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_firefox_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox for Android" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox for Android" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_fenix_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox for Android" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox for Android" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_fennec_aurora_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox for iOS" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox for iOS" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox for iOS" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Reference Browser" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_reference_browser_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox for Fire TV" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_tv_firefox_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox Reality" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_vrbrowser_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Lockwise for Android" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.mozilla_lockbox_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Lockwise for iOS" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_lockbox_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "mozregression" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_mozregression_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Burnham" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.burnham_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "mozphab" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.mozphab_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox for Echo Show" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_connect_firefox_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox Reality for PC-connected VR platforms" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_firefoxreality_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "mach" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.mozilla_mach_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox Focus for iOS" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_focus_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox Klar for iOS" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_klar_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox Focus for Android" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_focus_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox Focus for Android" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_focus_beta_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox Focus for Android" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_focus_nightly_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox Klar for Android" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_klar_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Bergamot Translator" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_bergamot_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox Translations" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.firefox_translations_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Mozilla VPN" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.mozillavpn_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Mozilla VPN" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_firefox_vpn_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Mozilla VPN" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Mozilla VPN" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_firefoxvpn_network_extension_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Mozilla VPN Cirrus Sidecar" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.mozillavpn_backend_cirrus_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Glean Dictionary" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.glean_dictionary_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Mozilla Developer Network" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.mdn_yari_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "www.mozilla.org" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.bedrock_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Viu Politica" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.viu_politica_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Treeherder" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.treeherder_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox Desktop background tasks" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.firefox_desktop_background_tasks_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
  -- FxA events are sent in a custom `accounts_events` ping
  -- Although they do not contain event metrics, this query monitors errors
  -- related to String metrics.
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox Accounts Frontend" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.accounts_frontend_stable.accounts_events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
  -- FxA events are sent in a custom `accounts_events` ping
  -- Although they do not contain event metrics, this query monitors errors
  -- related to String metrics.
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox Accounts Backend" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.accounts_backend_stable.accounts_events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox Monitor (Cirrus)" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.monitor_cirrus_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Glean Debug Ping Viewer" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.debug_ping_view_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Firefox Monitor (Frontend)" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.monitor_frontend_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Mozilla Social Mastodon Backend" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.moso_mastodon_backend_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "Mozilla Social Web App" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.moso_mastodon_web_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "TikTok Reporter (iOS)" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_tiktok_reporter_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
UNION ALL
  (
    WITH event_counters AS (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "TikTok Reporter (Android)" AS normalized_app_name,
        client_info.app_channel AS channel,
        metrics.labeled_counter
      FROM
        `moz-fx-data-shared-prod.org_mozilla_tiktokreporter_stable.events_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    )
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'overflow' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_overflow)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
    UNION ALL
    SELECT
      submission_date,
      normalized_app_name,
      channel,
      'invalid_value' AS error_type,
      KEY AS metric,
      COALESCE(SUM(value), 0) AS error_sum
    FROM
      event_counters,
      UNNEST(labeled_counter.glean_error_invalid_value)
    GROUP BY
      submission_date,
      normalized_app_name,
      channel,
      error_type,
      metric
  )
