SELECT channel, MAX(CAST(app_version AS INT64)) AS latest_version
  FROM
    (SELECT
      normalized_channel AS channel,
      SPLIT(application.version, '.')[OFFSET(0)] AS app_version,
      COUNT(*)
    FROM `moz-fx-data-shared-prod.telemetry_stable.main_v5`
    WHERE DATE(submission_timestamp) > DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
      AND normalized_channel IN ("nightly", "beta", "release")
    GROUP BY 1, 2
    HAVING COUNT(DISTINCT client_id) > 2000
    ORDER BY 1, 2 DESC)
GROUP BY 1
