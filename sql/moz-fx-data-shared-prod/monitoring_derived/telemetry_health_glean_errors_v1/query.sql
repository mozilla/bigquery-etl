-- Query for telemetry health glean errors across all applications
(
  WITH sample AS (
    SELECT
      "Firefox for Desktop" AS application,
      client_info.client_id,
      normalized_channel,
      DATE(submission_timestamp) AS submission_date,
      metrics.labeled_counter.glean_error_invalid_value AS ev,
      metrics.labeled_counter.glean_error_invalid_label AS el,
      metrics.labeled_counter.glean_error_invalid_state AS es,
      metrics.labeled_counter.glean_error_invalid_overflow AS eo
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_stable.metrics_v1`
    WHERE
      sample_id = 0
      AND DATE(submission_timestamp) = @submission_date
  ),
  -- Denominator: distinct clients per app and day
  app_day_totals AS (
    SELECT
      application,
      submission_date,
      normalized_channel,
      COUNT(DISTINCT client_id) AS total_clients
    FROM
      sample
    GROUP BY
      application,
      submission_date,
      normalized_channel
  ),
  -- Numerator per metric key: distinct clients with any error for that key on that day
  metric_clients_by_day AS (
    SELECT
      s.application,
      s.normalized_channel,
      s.submission_date,
      e.key AS metric_key,
      COUNT(DISTINCT s.client_id) AS clients_with_error
    FROM
      sample AS s
    JOIN
      UNNEST(ARRAY_CONCAT(IFNULL(ev, []), IFNULL(el, []), IFNULL(es, []), IFNULL(eo, []))) AS e
    WHERE
      NOT STARTS_WITH(e.key, 'glean')
      AND NOT STARTS_WITH(e.key, 'fog')
      AND e.value > 0
    GROUP BY
      s.application,
      s.submission_date,
      s.normalized_channel,
      metric_key
  )
  SELECT
    m.application,
    m.normalized_channel,
    m.submission_date,
    COUNTIF(SAFE_DIVIDE(m.clients_with_error, t.total_clients) > 0.01) AS num_metrics_over_1pct
  FROM
    metric_clients_by_day AS m
  JOIN
    app_day_totals AS t
    USING (application, submission_date, normalized_channel)
  GROUP BY
    m.application,
    m.submission_date,
    m.normalized_channel
)
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for Android" AS application,
        client_info.client_id,
        normalized_channel,
        DATE(submission_timestamp) AS submission_date,
        metrics.labeled_counter.glean_error_invalid_value AS ev,
        metrics.labeled_counter.glean_error_invalid_label AS el,
        metrics.labeled_counter.glean_error_invalid_state AS es,
        metrics.labeled_counter.glean_error_invalid_overflow AS eo
      FROM
        `moz-fx-data-shared-prod.org_mozilla_firefox_stable.metrics_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
    ),
  -- Denominator: distinct clients per app and day
    app_day_totals AS (
      SELECT
        application,
        submission_date,
        normalized_channel,
        COUNT(DISTINCT client_id) AS total_clients
      FROM
        sample
      GROUP BY
        application,
        submission_date,
        normalized_channel
    ),
  -- Numerator per metric key: distinct clients with any error for that key on that day
    metric_clients_by_day AS (
      SELECT
        s.application,
        s.normalized_channel,
        s.submission_date,
        e.key AS metric_key,
        COUNT(DISTINCT s.client_id) AS clients_with_error
      FROM
        sample AS s
      JOIN
        UNNEST(ARRAY_CONCAT(IFNULL(ev, []), IFNULL(el, []), IFNULL(es, []), IFNULL(eo, []))) AS e
      WHERE
        NOT STARTS_WITH(e.key, 'glean')
        AND NOT STARTS_WITH(e.key, 'fog')
        AND e.value > 0
      GROUP BY
        s.application,
        s.submission_date,
        s.normalized_channel,
        metric_key
    )
    SELECT
      m.application,
      m.normalized_channel,
      m.submission_date,
      COUNTIF(SAFE_DIVIDE(m.clients_with_error, t.total_clients) > 0.01) AS num_metrics_over_1pct
    FROM
      metric_clients_by_day AS m
    JOIN
      app_day_totals AS t
      USING (application, submission_date, normalized_channel)
    GROUP BY
      m.application,
      m.submission_date,
      m.normalized_channel
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for Android" AS application,
        client_info.client_id,
        normalized_channel,
        DATE(submission_timestamp) AS submission_date,
        metrics.labeled_counter.glean_error_invalid_value AS ev,
        metrics.labeled_counter.glean_error_invalid_label AS el,
        metrics.labeled_counter.glean_error_invalid_state AS es,
        metrics.labeled_counter.glean_error_invalid_overflow AS eo
      FROM
        `moz-fx-data-shared-prod.org_mozilla_firefox_beta_stable.metrics_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
    ),
  -- Denominator: distinct clients per app and day
    app_day_totals AS (
      SELECT
        application,
        submission_date,
        normalized_channel,
        COUNT(DISTINCT client_id) AS total_clients
      FROM
        sample
      GROUP BY
        application,
        submission_date,
        normalized_channel
    ),
  -- Numerator per metric key: distinct clients with any error for that key on that day
    metric_clients_by_day AS (
      SELECT
        s.application,
        s.normalized_channel,
        s.submission_date,
        e.key AS metric_key,
        COUNT(DISTINCT s.client_id) AS clients_with_error
      FROM
        sample AS s
      JOIN
        UNNEST(ARRAY_CONCAT(IFNULL(ev, []), IFNULL(el, []), IFNULL(es, []), IFNULL(eo, []))) AS e
      WHERE
        NOT STARTS_WITH(e.key, 'glean')
        AND NOT STARTS_WITH(e.key, 'fog')
        AND e.value > 0
      GROUP BY
        s.application,
        s.submission_date,
        s.normalized_channel,
        metric_key
    )
    SELECT
      m.application,
      m.normalized_channel,
      m.submission_date,
      COUNTIF(SAFE_DIVIDE(m.clients_with_error, t.total_clients) > 0.01) AS num_metrics_over_1pct
    FROM
      metric_clients_by_day AS m
    JOIN
      app_day_totals AS t
      USING (application, submission_date, normalized_channel)
    GROUP BY
      m.application,
      m.submission_date,
      m.normalized_channel
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for Android" AS application,
        client_info.client_id,
        normalized_channel,
        DATE(submission_timestamp) AS submission_date,
        metrics.labeled_counter.glean_error_invalid_value AS ev,
        metrics.labeled_counter.glean_error_invalid_label AS el,
        metrics.labeled_counter.glean_error_invalid_state AS es,
        metrics.labeled_counter.glean_error_invalid_overflow AS eo
      FROM
        `moz-fx-data-shared-prod.org_mozilla_fenix_stable.metrics_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
    ),
  -- Denominator: distinct clients per app and day
    app_day_totals AS (
      SELECT
        application,
        submission_date,
        normalized_channel,
        COUNT(DISTINCT client_id) AS total_clients
      FROM
        sample
      GROUP BY
        application,
        submission_date,
        normalized_channel
    ),
  -- Numerator per metric key: distinct clients with any error for that key on that day
    metric_clients_by_day AS (
      SELECT
        s.application,
        s.normalized_channel,
        s.submission_date,
        e.key AS metric_key,
        COUNT(DISTINCT s.client_id) AS clients_with_error
      FROM
        sample AS s
      JOIN
        UNNEST(ARRAY_CONCAT(IFNULL(ev, []), IFNULL(el, []), IFNULL(es, []), IFNULL(eo, []))) AS e
      WHERE
        NOT STARTS_WITH(e.key, 'glean')
        AND NOT STARTS_WITH(e.key, 'fog')
        AND e.value > 0
      GROUP BY
        s.application,
        s.submission_date,
        s.normalized_channel,
        metric_key
    )
    SELECT
      m.application,
      m.normalized_channel,
      m.submission_date,
      COUNTIF(SAFE_DIVIDE(m.clients_with_error, t.total_clients) > 0.01) AS num_metrics_over_1pct
    FROM
      metric_clients_by_day AS m
    JOIN
      app_day_totals AS t
      USING (application, submission_date, normalized_channel)
    GROUP BY
      m.application,
      m.submission_date,
      m.normalized_channel
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for iOS" AS application,
        client_info.client_id,
        normalized_channel,
        DATE(submission_timestamp) AS submission_date,
        metrics.labeled_counter.glean_error_invalid_value AS ev,
        metrics.labeled_counter.glean_error_invalid_label AS el,
        metrics.labeled_counter.glean_error_invalid_state AS es,
        metrics.labeled_counter.glean_error_invalid_overflow AS eo
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_firefox_stable.metrics_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
    ),
  -- Denominator: distinct clients per app and day
    app_day_totals AS (
      SELECT
        application,
        submission_date,
        normalized_channel,
        COUNT(DISTINCT client_id) AS total_clients
      FROM
        sample
      GROUP BY
        application,
        submission_date,
        normalized_channel
    ),
  -- Numerator per metric key: distinct clients with any error for that key on that day
    metric_clients_by_day AS (
      SELECT
        s.application,
        s.normalized_channel,
        s.submission_date,
        e.key AS metric_key,
        COUNT(DISTINCT s.client_id) AS clients_with_error
      FROM
        sample AS s
      JOIN
        UNNEST(ARRAY_CONCAT(IFNULL(ev, []), IFNULL(el, []), IFNULL(es, []), IFNULL(eo, []))) AS e
      WHERE
        NOT STARTS_WITH(e.key, 'glean')
        AND NOT STARTS_WITH(e.key, 'fog')
        AND e.value > 0
      GROUP BY
        s.application,
        s.submission_date,
        s.normalized_channel,
        metric_key
    )
    SELECT
      m.application,
      m.normalized_channel,
      m.submission_date,
      COUNTIF(SAFE_DIVIDE(m.clients_with_error, t.total_clients) > 0.01) AS num_metrics_over_1pct
    FROM
      metric_clients_by_day AS m
    JOIN
      app_day_totals AS t
      USING (application, submission_date, normalized_channel)
    GROUP BY
      m.application,
      m.submission_date,
      m.normalized_channel
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for iOS" AS application,
        client_info.client_id,
        normalized_channel,
        DATE(submission_timestamp) AS submission_date,
        metrics.labeled_counter.glean_error_invalid_value AS ev,
        metrics.labeled_counter.glean_error_invalid_label AS el,
        metrics.labeled_counter.glean_error_invalid_state AS es,
        metrics.labeled_counter.glean_error_invalid_overflow AS eo
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_stable.metrics_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
    ),
  -- Denominator: distinct clients per app and day
    app_day_totals AS (
      SELECT
        application,
        submission_date,
        normalized_channel,
        COUNT(DISTINCT client_id) AS total_clients
      FROM
        sample
      GROUP BY
        application,
        submission_date,
        normalized_channel
    ),
  -- Numerator per metric key: distinct clients with any error for that key on that day
    metric_clients_by_day AS (
      SELECT
        s.application,
        s.normalized_channel,
        s.submission_date,
        e.key AS metric_key,
        COUNT(DISTINCT s.client_id) AS clients_with_error
      FROM
        sample AS s
      JOIN
        UNNEST(ARRAY_CONCAT(IFNULL(ev, []), IFNULL(el, []), IFNULL(es, []), IFNULL(eo, []))) AS e
      WHERE
        NOT STARTS_WITH(e.key, 'glean')
        AND NOT STARTS_WITH(e.key, 'fog')
        AND e.value > 0
      GROUP BY
        s.application,
        s.submission_date,
        s.normalized_channel,
        metric_key
    )
    SELECT
      m.application,
      m.normalized_channel,
      m.submission_date,
      COUNTIF(SAFE_DIVIDE(m.clients_with_error, t.total_clients) > 0.01) AS num_metrics_over_1pct
    FROM
      metric_clients_by_day AS m
    JOIN
      app_day_totals AS t
      USING (application, submission_date, normalized_channel)
    GROUP BY
      m.application,
      m.submission_date,
      m.normalized_channel
  )
UNION ALL
  (
    WITH sample AS (
      SELECT
        "Firefox for iOS" AS application,
        client_info.client_id,
        normalized_channel,
        DATE(submission_timestamp) AS submission_date,
        metrics.labeled_counter.glean_error_invalid_value AS ev,
        metrics.labeled_counter.glean_error_invalid_label AS el,
        metrics.labeled_counter.glean_error_invalid_state AS es,
        metrics.labeled_counter.glean_error_invalid_overflow AS eo
      FROM
        `moz-fx-data-shared-prod.org_mozilla_ios_fennec_stable.metrics_v1`
      WHERE
        sample_id = 0
        AND DATE(submission_timestamp) = @submission_date
    ),
  -- Denominator: distinct clients per app and day
    app_day_totals AS (
      SELECT
        application,
        submission_date,
        normalized_channel,
        COUNT(DISTINCT client_id) AS total_clients
      FROM
        sample
      GROUP BY
        application,
        submission_date,
        normalized_channel
    ),
  -- Numerator per metric key: distinct clients with any error for that key on that day
    metric_clients_by_day AS (
      SELECT
        s.application,
        s.normalized_channel,
        s.submission_date,
        e.key AS metric_key,
        COUNT(DISTINCT s.client_id) AS clients_with_error
      FROM
        sample AS s
      JOIN
        UNNEST(ARRAY_CONCAT(IFNULL(ev, []), IFNULL(el, []), IFNULL(es, []), IFNULL(eo, []))) AS e
      WHERE
        NOT STARTS_WITH(e.key, 'glean')
        AND NOT STARTS_WITH(e.key, 'fog')
        AND e.value > 0
      GROUP BY
        s.application,
        s.submission_date,
        s.normalized_channel,
        metric_key
    )
    SELECT
      m.application,
      m.normalized_channel,
      m.submission_date,
      COUNTIF(SAFE_DIVIDE(m.clients_with_error, t.total_clients) > 0.01) AS num_metrics_over_1pct
    FROM
      metric_clients_by_day AS m
    JOIN
      app_day_totals AS t
      USING (application, submission_date, normalized_channel)
    GROUP BY
      m.application,
      m.submission_date,
      m.normalized_channel
  )
