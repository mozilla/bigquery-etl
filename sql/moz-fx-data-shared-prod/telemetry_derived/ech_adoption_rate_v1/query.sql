WITH unioned AS (
  /* ---------- http3_ech_outcome ---------- */
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'http3_ech_outcome' AS metric,
    h.key AS label,
    v.key AS key,
    v.value AS value,
    CASE
      WHEN COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.geo.country
      ELSE 'OTHER'
    END AS country_code,
    COUNT(DISTINCT client_info.client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`,
    UNNEST(metrics.labeled_custom_distribution.http3_ech_outcome) AS h,
    UNNEST(h.value.values) AS v
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND metrics.labeled_timing_distribution.network_sup_http3_tcp_connection IS NOT NULL
  GROUP BY
    submission_date,
    label,
    key,
    value,
    metadata.geo.country
  UNION ALL
  /* ---------- ssl_handshake_result_ech ---------- */
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'ssl_handshake_result_ech' AS metric,
    "" AS label,
    s.key AS key,
    s.value AS value,
    CASE
      WHEN COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.geo.country
      ELSE 'OTHER'
    END AS country_code,
    COUNT(DISTINCT client_info.client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`,
    UNNEST(metrics.custom_distribution.ssl_handshake_result_ech.values) AS s
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND metrics.labeled_timing_distribution.network_sup_http3_tcp_connection IS NOT NULL
  GROUP BY
    submission_date,
    label,
    key,
    value,
    metadata.geo.country
  UNION ALL
  /* ---------- ssl_handshake_result_ech_grease ---------- */
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'ssl_handshake_result_ech_grease' AS metric,
    "" AS label,
    s.key AS key,
    s.value AS value,
    CASE
      WHEN COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.geo.country
      ELSE 'OTHER'
    END AS country_code,
    COUNT(DISTINCT client_info.client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`,
    UNNEST(metrics.custom_distribution.ssl_handshake_result_ech_grease.values) AS s
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND metrics.labeled_timing_distribution.network_sup_http3_tcp_connection IS NOT NULL
  GROUP BY
    submission_date,
    label,
    key,
    value,
    metadata.geo.country
  UNION ALL
  /* ---------- ssl_handshake_privacy ---------- */
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'ssl_handshake_privacy' AS metric,
    "" AS label,
    s.key AS key,
    s.value AS value,
    CASE
      WHEN COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.geo.country
      ELSE 'OTHER'
    END AS country_code,
    COUNT(DISTINCT client_info.client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`,
    UNNEST(metrics.custom_distribution.ssl_handshake_privacy.values) AS s
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND metrics.labeled_timing_distribution.network_sup_http3_tcp_connection IS NOT NULL
  GROUP BY
    submission_date,
    label,
    key,
    value,
    metadata.geo.country
)
SELECT
  submission_date,
  metric,
  label,
  key,
  value,
  country_code,
  SUM(client_count) AS total_client_count
FROM
  unioned
GROUP BY
  submission_date,
  metric,
  label,
  key,
  value,
  country_code
ORDER BY
  submission_date DESC,
  metric,
  country_code,
  key,
  value
