-- Query for telemetry health glean metric errors across all applications
(
  WITH sample AS (
    SELECT
      "firefox_desktop" AS app_id,
      "Firefox for Desktop" AS application,
      client_info.client_id,
      COALESCE(normalized_channel, 'Other') AS normalized_channel,
      DATE(submission_timestamp) AS submission_date,
      metrics,
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`
    WHERE
      DATE(submission_timestamp) = @submission_date
  ),
  errors AS (
    SELECT
      submission_date,
      app_id,
      application,
      normalized_channel,
      e.error_type,
      label.key AS metric,
      SUM(label.value) AS counts,
      COUNT(DISTINCT client_id) AS affected_clients
    FROM
      sample
    CROSS JOIN
      UNNEST(
        [
          STRUCT(
            'Invalid Label' AS error_type,
            metrics.labeled_counter.glean_error_invalid_label AS errors
          ),
          ('Invalid Value', metrics.labeled_counter.glean_error_invalid_value),
          ('Invalid State', metrics.labeled_counter.glean_error_invalid_state),
          ('Invalid Overflow', metrics.labeled_counter.glean_error_invalid_overflow)
        ]
      ) AS e
    CROSS JOIN
      UNNEST(e.errors) AS label
    GROUP BY
      ALL
  ),
  all_clients AS (
    SELECT
      submission_date,
      app_id,
      application,
      normalized_channel,
      COUNT(DISTINCT client_id) AS total_clients
    FROM
      sample
    GROUP BY
      ALL
  )
  SELECT
    d.submission_date,
    d.app_id,
    d.application,
    d.normalized_channel,
    d.error_type,
    d.metric,
    d.counts,
    d.affected_clients,
    d.affected_clients / c.total_clients AS fraction_affected_clients,
    c.total_clients,
  FROM
    errors d
  INNER JOIN
    all_clients c
    USING (submission_date, app_id, application, normalized_channel)
)
UNION ALL
  (
    WITH sample AS (
      SELECT
        "org_mozilla_firefox" AS app_id,
        "Firefox for Android" AS application,
        client_info.client_id,
        mozfun.norm.fenix_app_info(
          "org_mozilla_firefox",
          client_info.app_build
        ).channel AS normalized_channel,
        DATE(submission_timestamp) AS submission_date,
        metrics,
      FROM
        `moz-fx-data-shared-prod.org_mozilla_firefox_stable.metrics_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    ),
    errors AS (
      SELECT
        submission_date,
        app_id,
        application,
        normalized_channel,
        e.error_type,
        label.key AS metric,
        SUM(label.value) AS counts,
        COUNT(DISTINCT client_id) AS affected_clients
      FROM
        sample
      CROSS JOIN
        UNNEST(
          [
            STRUCT(
              'Invalid Label' AS error_type,
              metrics.labeled_counter.glean_error_invalid_label AS errors
            ),
            ('Invalid Value', metrics.labeled_counter.glean_error_invalid_value),
            ('Invalid State', metrics.labeled_counter.glean_error_invalid_state),
            ('Invalid Overflow', metrics.labeled_counter.glean_error_invalid_overflow)
          ]
        ) AS e
      CROSS JOIN
        UNNEST(e.errors) AS label
      GROUP BY
        ALL
    ),
    all_clients AS (
      SELECT
        submission_date,
        app_id,
        application,
        normalized_channel,
        COUNT(DISTINCT client_id) AS total_clients
      FROM
        sample
      GROUP BY
        ALL
    )
    SELECT
      d.submission_date,
      d.app_id,
      d.application,
      d.normalized_channel,
      d.error_type,
      d.metric,
      d.counts,
      d.affected_clients,
      d.affected_clients / c.total_clients AS fraction_affected_clients,
      c.total_clients,
    FROM
      errors d
    INNER JOIN
      all_clients c
      USING (submission_date, app_id, application, normalized_channel)
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "org_mozilla_firefox_beta" AS app_id,
        "Firefox for Android" AS application,
        client_info.client_id,
        mozfun.norm.fenix_app_info(
          "org_mozilla_firefox_beta",
          client_info.app_build
        ).channel AS normalized_channel,
        DATE(submission_timestamp) AS submission_date,
        metrics,
      FROM
        `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.metrics_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    ),
    errors AS (
      SELECT
        submission_date,
        app_id,
        application,
        normalized_channel,
        e.error_type,
        label.key AS metric,
        SUM(label.value) AS counts,
        COUNT(DISTINCT client_id) AS affected_clients
      FROM
        sample
      CROSS JOIN
        UNNEST(
          [
            STRUCT(
              'Invalid Label' AS error_type,
              metrics.labeled_counter.glean_error_invalid_label AS errors
            ),
            ('Invalid Value', metrics.labeled_counter.glean_error_invalid_value),
            ('Invalid State', metrics.labeled_counter.glean_error_invalid_state),
            ('Invalid Overflow', metrics.labeled_counter.glean_error_invalid_overflow)
          ]
        ) AS e
      CROSS JOIN
        UNNEST(e.errors) AS label
      GROUP BY
        ALL
    ),
    all_clients AS (
      SELECT
        submission_date,
        app_id,
        application,
        normalized_channel,
        COUNT(DISTINCT client_id) AS total_clients
      FROM
        sample
      GROUP BY
        ALL
    )
    SELECT
      d.submission_date,
      d.app_id,
      d.application,
      d.normalized_channel,
      d.error_type,
      d.metric,
      d.counts,
      d.affected_clients,
      d.affected_clients / c.total_clients AS fraction_affected_clients,
      c.total_clients,
    FROM
      errors d
    INNER JOIN
      all_clients c
      USING (submission_date, app_id, application, normalized_channel)
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "org_mozilla_fenix" AS app_id,
        "Firefox for Android" AS application,
        client_info.client_id,
        mozfun.norm.fenix_app_info(
          "org_mozilla_fenix",
          client_info.app_build
        ).channel AS normalized_channel,
        DATE(submission_timestamp) AS submission_date,
        metrics,
      FROM
        `moz-fx-data-shared-prod.org_mozilla_fenix_stable.metrics_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    ),
    errors AS (
      SELECT
        submission_date,
        app_id,
        application,
        normalized_channel,
        e.error_type,
        label.key AS metric,
        SUM(label.value) AS counts,
        COUNT(DISTINCT client_id) AS affected_clients
      FROM
        sample
      CROSS JOIN
        UNNEST(
          [
            STRUCT(
              'Invalid Label' AS error_type,
              metrics.labeled_counter.glean_error_invalid_label AS errors
            ),
            ('Invalid Value', metrics.labeled_counter.glean_error_invalid_value),
            ('Invalid State', metrics.labeled_counter.glean_error_invalid_state),
            ('Invalid Overflow', metrics.labeled_counter.glean_error_invalid_overflow)
          ]
        ) AS e
      CROSS JOIN
        UNNEST(e.errors) AS label
      GROUP BY
        ALL
    ),
    all_clients AS (
      SELECT
        submission_date,
        app_id,
        application,
        normalized_channel,
        COUNT(DISTINCT client_id) AS total_clients
      FROM
        sample
      GROUP BY
        ALL
    )
    SELECT
      d.submission_date,
      d.app_id,
      d.application,
      d.normalized_channel,
      d.error_type,
      d.metric,
      d.counts,
      d.affected_clients,
      d.affected_clients / c.total_clients AS fraction_affected_clients,
      c.total_clients,
    FROM
      errors d
    INNER JOIN
      all_clients c
      USING (submission_date, app_id, application, normalized_channel)
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "org_mozilla_ios_firefox" AS app_id,
        "Firefox for iOS" AS application,
        client_info.client_id,
        COALESCE(normalized_channel, 'Other') AS normalized_channel,
        DATE(submission_timestamp) AS submission_date,
        metrics,
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.metrics_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    ),
    errors AS (
      SELECT
        submission_date,
        app_id,
        application,
        normalized_channel,
        e.error_type,
        label.key AS metric,
        SUM(label.value) AS counts,
        COUNT(DISTINCT client_id) AS affected_clients
      FROM
        sample
      CROSS JOIN
        UNNEST(
          [
            STRUCT(
              'Invalid Label' AS error_type,
              metrics.labeled_counter.glean_error_invalid_label AS errors
            ),
            ('Invalid Value', metrics.labeled_counter.glean_error_invalid_value),
            ('Invalid State', metrics.labeled_counter.glean_error_invalid_state),
            ('Invalid Overflow', metrics.labeled_counter.glean_error_invalid_overflow)
          ]
        ) AS e
      CROSS JOIN
        UNNEST(e.errors) AS label
      GROUP BY
        ALL
    ),
    all_clients AS (
      SELECT
        submission_date,
        app_id,
        application,
        normalized_channel,
        COUNT(DISTINCT client_id) AS total_clients
      FROM
        sample
      GROUP BY
        ALL
    )
    SELECT
      d.submission_date,
      d.app_id,
      d.application,
      d.normalized_channel,
      d.error_type,
      d.metric,
      d.counts,
      d.affected_clients,
      d.affected_clients / c.total_clients AS fraction_affected_clients,
      c.total_clients,
    FROM
      errors d
    INNER JOIN
      all_clients c
      USING (submission_date, app_id, application, normalized_channel)
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "org_mozilla_ios_firefoxbeta" AS app_id,
        "Firefox for iOS" AS application,
        client_info.client_id,
        COALESCE(normalized_channel, 'Other') AS normalized_channel,
        DATE(submission_timestamp) AS submission_date,
        metrics,
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.metrics_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    ),
    errors AS (
      SELECT
        submission_date,
        app_id,
        application,
        normalized_channel,
        e.error_type,
        label.key AS metric,
        SUM(label.value) AS counts,
        COUNT(DISTINCT client_id) AS affected_clients
      FROM
        sample
      CROSS JOIN
        UNNEST(
          [
            STRUCT(
              'Invalid Label' AS error_type,
              metrics.labeled_counter.glean_error_invalid_label AS errors
            ),
            ('Invalid Value', metrics.labeled_counter.glean_error_invalid_value),
            ('Invalid State', metrics.labeled_counter.glean_error_invalid_state),
            ('Invalid Overflow', metrics.labeled_counter.glean_error_invalid_overflow)
          ]
        ) AS e
      CROSS JOIN
        UNNEST(e.errors) AS label
      GROUP BY
        ALL
    ),
    all_clients AS (
      SELECT
        submission_date,
        app_id,
        application,
        normalized_channel,
        COUNT(DISTINCT client_id) AS total_clients
      FROM
        sample
      GROUP BY
        ALL
    )
    SELECT
      d.submission_date,
      d.app_id,
      d.application,
      d.normalized_channel,
      d.error_type,
      d.metric,
      d.counts,
      d.affected_clients,
      d.affected_clients / c.total_clients AS fraction_affected_clients,
      c.total_clients,
    FROM
      errors d
    INNER JOIN
      all_clients c
      USING (submission_date, app_id, application, normalized_channel)
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "org_mozilla_ios_fennec" AS app_id,
        "Firefox for iOS" AS application,
        client_info.client_id,
        COALESCE(normalized_channel, 'Other') AS normalized_channel,
        DATE(submission_timestamp) AS submission_date,
        metrics,
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.metrics_v1`
      WHERE
        DATE(submission_timestamp) = @submission_date
    ),
    errors AS (
      SELECT
        submission_date,
        app_id,
        application,
        normalized_channel,
        e.error_type,
        label.key AS metric,
        SUM(label.value) AS counts,
        COUNT(DISTINCT client_id) AS affected_clients
      FROM
        sample
      CROSS JOIN
        UNNEST(
          [
            STRUCT(
              'Invalid Label' AS error_type,
              metrics.labeled_counter.glean_error_invalid_label AS errors
            ),
            ('Invalid Value', metrics.labeled_counter.glean_error_invalid_value),
            ('Invalid State', metrics.labeled_counter.glean_error_invalid_state),
            ('Invalid Overflow', metrics.labeled_counter.glean_error_invalid_overflow)
          ]
        ) AS e
      CROSS JOIN
        UNNEST(e.errors) AS label
      GROUP BY
        ALL
    ),
    all_clients AS (
      SELECT
        submission_date,
        app_id,
        application,
        normalized_channel,
        COUNT(DISTINCT client_id) AS total_clients
      FROM
        sample
      GROUP BY
        ALL
    )
    SELECT
      d.submission_date,
      d.app_id,
      d.application,
      d.normalized_channel,
      d.error_type,
      d.metric,
      d.counts,
      d.affected_clients,
      d.affected_clients / c.total_clients AS fraction_affected_clients,
      c.total_clients,
    FROM
      errors d
    INNER JOIN
      all_clients c
      USING (submission_date, app_id, application, normalized_channel)
  )
