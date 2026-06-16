WITH counts AS (
  SELECT
    submission_date,
    application,
    channel,
    engine_name,
    COUNT(*) AS count_total,
    COUNTIF(
      failure_reason_list IS NULL
      AND (incoming_counts IS NOT NULL OR outgoing_counts IS NOT NULL)
    ) AS count_success,
    COUNTIF(failure_reason_list IS NOT NULL) AS count_errors,
    SUM((SELECT value FROM UNNEST(incoming_counts) WHERE KEY = "applied")) AS applied_count,
    SUM((SELECT value FROM UNNEST(incoming_counts) WHERE KEY = "reconciled")) AS reconciled_count,
    SUM(
      (SELECT value FROM UNNEST(incoming_counts) WHERE KEY = "failed_to_apply")
    ) AS failed_to_apply_count,
    SUM((SELECT value FROM UNNEST(outgoing_counts) WHERE KEY = "uploaded")) AS uploaded_count,
    SUM(
      (SELECT value FROM UNNEST(outgoing_counts) WHERE KEY = "failed_to_upload")
    ) AS failed_to_upload_count
  FROM
    (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-android" AS application,
        "nightly" AS channel,
        "logins" AS engine_name,
        metrics.labeled_string.logins_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.logins_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.logins_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_fenix.logins_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-android" AS application,
        "beta" AS channel,
        "logins" AS engine_name,
        metrics.labeled_string.logins_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.logins_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.logins_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_firefox_beta.logins_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-android" AS application,
        "release" AS channel,
        "logins" AS engine_name,
        metrics.labeled_string.logins_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.logins_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.logins_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_firefox.logins_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-ios" AS application,
        "beta" AS channel,
        "logins" AS engine_name,
        metrics.labeled_string.logins_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.logins_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.logins_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_ios_firefoxbeta.logins_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-ios" AS application,
        "release" AS channel,
        "logins" AS engine_name,
        metrics.labeled_string.logins_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.logins_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.logins_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_ios_firefox.logins_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-android" AS application,
        "nightly" AS channel,
        "bookmarks" AS engine_name,
        metrics.labeled_string.bookmarks_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.bookmarks_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.bookmarks_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_fenix.bookmarks_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-android" AS application,
        "beta" AS channel,
        "bookmarks" AS engine_name,
        metrics.labeled_string.bookmarks_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.bookmarks_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.bookmarks_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_firefox_beta.bookmarks_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-android" AS application,
        "release" AS channel,
        "bookmarks" AS engine_name,
        metrics.labeled_string.bookmarks_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.bookmarks_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.bookmarks_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_firefox.bookmarks_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-ios" AS application,
        "beta" AS channel,
        "bookmarks" AS engine_name,
        metrics.labeled_string.bookmarks_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.bookmarks_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.bookmarks_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_ios_firefoxbeta.bookmarks_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-ios" AS application,
        "release" AS channel,
        "bookmarks" AS engine_name,
        metrics.labeled_string.bookmarks_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.bookmarks_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.bookmarks_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_ios_firefox.bookmarks_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-android" AS application,
        "nightly" AS channel,
        "addresses" AS engine_name,
        metrics.labeled_string.addresses_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.addresses_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.addresses_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_fenix.addresses_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-android" AS application,
        "nightly" AS channel,
        "creditcards" AS engine_name,
        metrics.labeled_string.creditcards_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.creditcards_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.creditcards_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_fenix.creditcards_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-android" AS application,
        "beta" AS channel,
        "creditcards" AS engine_name,
        metrics.labeled_string.creditcards_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.creditcards_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.creditcards_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_firefox_beta.creditcards_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-android" AS application,
        "release" AS channel,
        "creditcards" AS engine_name,
        metrics.labeled_string.creditcards_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.creditcards_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.creditcards_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_firefox.creditcards_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-ios" AS application,
        "beta" AS channel,
        "creditcards" AS engine_name,
        metrics.labeled_string.creditcards_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.creditcards_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.creditcards_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_ios_firefoxbeta.creditcards_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-ios" AS application,
        "release" AS channel,
        "creditcards" AS engine_name,
        metrics.labeled_string.creditcards_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.creditcards_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.creditcards_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_ios_firefox.creditcards_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-android" AS application,
        "nightly" AS channel,
        "history" AS engine_name,
        metrics.labeled_string.history_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.history_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.history_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_fenix.history_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-android" AS application,
        "beta" AS channel,
        "history" AS engine_name,
        metrics.labeled_string.history_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.history_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.history_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_firefox_beta.history_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-android" AS application,
        "release" AS channel,
        "history" AS engine_name,
        metrics.labeled_string.history_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.history_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.history_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_firefox.history_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-ios" AS application,
        "beta" AS channel,
        "history" AS engine_name,
        metrics.labeled_string.history_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.history_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.history_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_ios_firefoxbeta.history_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-ios" AS application,
        "release" AS channel,
        "history" AS engine_name,
        metrics.labeled_string.history_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.history_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.history_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_ios_firefox.history_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-android" AS application,
        "nightly" AS channel,
        "tabs" AS engine_name,
        metrics.labeled_string.tabs_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.tabs_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.tabs_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_fenix.tabs_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-android" AS application,
        "beta" AS channel,
        "tabs" AS engine_name,
        metrics.labeled_string.tabs_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.tabs_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.tabs_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_firefox_beta.tabs_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-android" AS application,
        "release" AS channel,
        "tabs" AS engine_name,
        metrics.labeled_string.tabs_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.tabs_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.tabs_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_firefox.tabs_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-ios" AS application,
        "beta" AS channel,
        "tabs" AS engine_name,
        metrics.labeled_string.tabs_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.tabs_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.tabs_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_ios_firefoxbeta.tabs_sync
      UNION ALL
      SELECT
        DATE(submission_timestamp) AS submission_date,
        "firefox-ios" AS application,
        "release" AS channel,
        "tabs" AS engine_name,
        metrics.labeled_string.tabs_sync_v2_failure_reason AS failure_reason_list,
        metrics.labeled_counter.tabs_sync_v2_incoming AS incoming_counts,
        metrics.labeled_counter.tabs_sync_v2_outgoing AS outgoing_counts,
      FROM
        mozdata.org_mozilla_ios_firefox.tabs_sync
    )
  GROUP BY
    application,
    channel,
    engine_name,
    submission_date
)
SELECT
  submission_date,
  application,
  channel,
  engine_name,
  count_total,
  count_success,
  count_errors,
  applied_count,
  reconciled_count,
  failed_to_apply_count,
  uploaded_count,
  failed_to_upload_count,
  SAFE_DIVIDE(count_success, count_success + count_errors) * 100 AS success_rate,
  (count_total - count_errors) * 100 / count_total AS non_error_rate,
  AVG(count_total) OVER (
    PARTITION BY
      engine_name
    ORDER BY
      submission_date
    ROWS BETWEEN
      6 PRECEDING
      AND CURRENT ROW
  ) AS total_count_moving_avg
FROM
  counts
WHERE
  submission_date = @submission_date
    -- This filters out engines like addresses that have counts in the dozens
    -- with the exception of Firefox iOS Beta where
    -- that is the norm.  These engines are only enabled via some weird process and aren't worth monitoring.  It also filters
    -- out cosmic-ray bit-flipped engines like l%gins.
  AND
  CASE
    WHEN channel = 'beta'
      AND application = 'firefox-ios'
      THEN count_success + count_errors > 0
    ELSE count_success + count_errors > 100
  END
