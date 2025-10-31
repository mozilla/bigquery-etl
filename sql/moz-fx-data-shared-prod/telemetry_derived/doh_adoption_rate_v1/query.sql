WITH unioned AS (
  /* ---------- doh.evaluate_v2_heuristics ---------- */
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'doh.evaluate_v2_heuristics' AS metric,
    "value" AS key,
    CASE
      WHEN COUNT(DISTINCT client_id) >= 5000
        THEN normalized_country_code
      ELSE 'OTHER'
    END AS country_code,
    COUNT(DISTINCT client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.events_stream`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND event_category = 'doh'
    AND event_name = 'evaluate_v2_heuristics'
  GROUP BY
    submission_date,
    key,
    normalized_country_code
  UNION ALL
  /* ---------- doh.state_disabled ---------- */
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'doh.state_disabled' AS metric,
    '' AS key,
    CASE
      WHEN COUNT(DISTINCT client_id) >= 5000
        THEN normalized_country_code
      ELSE 'OTHER'
    END AS country_code,
    COUNT(DISTINCT client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.events_stream`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND event_category = 'doh'
    AND event_name = 'state_disabled'
  GROUP BY
    submission_date,
    key,
    normalized_country_code
  UNION ALL
  /* ---------- doh.state_enabled ---------- */
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'doh.state_enabled' AS metric,
    '' AS key,
    CASE
      WHEN COUNT(DISTINCT client_id) >= 5000
        THEN normalized_country_code
      ELSE 'OTHER'
    END AS country_code,
    COUNT(DISTINCT client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.events_stream`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND event_category = 'doh'
    AND event_name = 'state_enabled'
  GROUP BY
    submission_date,
    key,
    normalized_country_code
  UNION ALL
  /* ---------- doh.state_manually_disabled ---------- */
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'doh.state_manually_disabled' AS metric,
    '' AS key,
    CASE
      WHEN COUNT(DISTINCT client_id) >= 5000
        THEN normalized_country_code
      ELSE 'OTHER'
    END AS country_code,
    COUNT(DISTINCT client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.events_stream`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND event_category = 'doh'
    AND event_name = 'state_manually_disabled'
  GROUP BY
    submission_date,
    key,
    normalized_country_code
  UNION ALL
  /* ---------- doh.state_policy_disabled ---------- */
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'doh.state_policy_disabled' AS metric,
    '' AS key,
    CASE
      WHEN COUNT(DISTINCT client_id) >= 5000
        THEN normalized_country_code
      ELSE 'OTHER'
    END AS country_code,
    COUNT(DISTINCT client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.events_stream`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND event_category = 'doh'
    AND event_name = 'state_policy_disabled'
  GROUP BY
    submission_date,
    key,
    normalized_country_code
  UNION ALL
  /* ---------- networking.doh_heuristics_attempts ---------- */
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'networking.doh_heuristics_attempts' AS metric,
    'networking.doh_heuristics_attempts' AS key,
    CASE
      WHEN COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.geo.country
      ELSE 'OTHER'
    END AS country_code,
    COUNT(DISTINCT client_info.client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND metrics.counter.networking_doh_heuristics_attempts IS NOT NULL
  GROUP BY
    submission_date,
    key,
    metadata.geo.country
  UNION ALL
  /* ---------- networking.doh_heuristics_pass_count ---------- */
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'networking.doh_heuristics_pass_count' AS metric,
    'networking.doh_heuristics_attempts' AS key,
    CASE
      WHEN COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.geo.country
      ELSE 'OTHER'
    END AS country_code,
    COUNT(DISTINCT client_info.client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND metrics.counter.networking_doh_heuristics_pass_count IS NOT NULL
  GROUP BY
    submission_date,
    key,
    metadata.geo.country
  UNION ALL
  /* ---------- networking.doh_heuristics_result ---------- */
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'networking.doh_heuristics_result' AS metric,
    'networking.doh_heuristics_result' AS key,
    CASE
      WHEN COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.geo.country
      ELSE 'OTHER'
    END AS country_code,
    COUNT(DISTINCT client_info.client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND metrics.quantity.networking_doh_heuristics_result IS NOT NULL
  GROUP BY
    submission_date,
    key,
    metadata.geo.country
  UNION ALL
  /* ---------- security.doh.settings.provider_choice_value ---------- */
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'security.doh.settings.provider_choice_value' AS metric,
    "value" AS key,
    CASE
      WHEN COUNT(DISTINCT client_id) >= 5000
        THEN normalized_country_code
      ELSE 'OTHER'
    END AS country_code,
    COUNT(DISTINCT client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.events_stream`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND event_category = 'security.doh.settings'
    AND event_name = 'provider_choice_value'
  GROUP BY
    submission_date,
    key,
    normalized_country_code
)
SELECT
  submission_date,
  metric,
  key,
  country_code,
  SUM(client_count) AS total_client_count
FROM
  unioned
GROUP BY
  submission_date,
  metric,
  key,
  country_code
ORDER BY
  submission_date DESC,
  metric,
  country_code,
  key
