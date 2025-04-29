WITH unioned AS (
  /* ---------- http3_ech_outcome ---------- */
  SELECT
    DATE(submission_timestamp) AS day,
    'http3_ech_outcome' AS metric,
    h.key AS key,
    CASE
      WHEN COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.geo.country
      ELSE 'OTHER'
    END AS country,
    CASE
      WHEN COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.isp.name
      ELSE 'OTHER'
    END AS isp_name,
    COUNT(DISTINCT client_info.client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`,
    UNNEST(metrics.labeled_custom_distribution.http3_ech_outcome) AS h
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    AND CURRENT_DATE()
    AND client_info.client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND metrics.labeled_timing_distribution.network_sup_http3_tcp_connection IS NOT NULL
  GROUP BY
    day,
    key,
    metadata.geo.country,
    metadata.isp.name
  UNION ALL
  /* ---------- ssl_handshake_result_ech ---------- */
  SELECT
    DATE(submission_timestamp) AS day,
    'ssl_handshake_result_ech' AS metric,
    "" AS key,
    CASE
      WHEN COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.geo.country
      ELSE 'OTHER'
    END AS country,
    CASE
      WHEN COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.isp.name
      ELSE 'OTHER'
    END AS isp_name,
    COUNT(DISTINCT client_info.client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`,
    UNNEST(metrics.custom_distribution.ssl_handshake_result_ech.values) AS s
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    AND CURRENT_DATE()
    AND client_info.client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND metrics.labeled_timing_distribution.network_sup_http3_tcp_connection IS NOT NULL
  GROUP BY
    day,
    key,
    metadata.geo.country,
    metadata.isp.name
  UNION ALL
  /* ---------- ssl_handshake_result_ech_grease ---------- */
  SELECT
    DATE(submission_timestamp) AS day,
    'ssl_handshake_result_ech_grease' AS metric,
    "" AS key,
    CASE
      WHEN COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.geo.country
      ELSE 'OTHER'
    END AS country,
    CASE
      WHEN COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.isp.name
      ELSE 'OTHER'
    END AS isp_name,
    COUNT(DISTINCT client_info.client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`,
    UNNEST(metrics.custom_distribution.ssl_handshake_result_ech_grease.values) AS s
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    AND CURRENT_DATE()
    AND client_info.client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND metrics.labeled_timing_distribution.network_sup_http3_tcp_connection IS NOT NULL
  GROUP BY
    day,
    key,
    metadata.geo.country,
    metadata.isp.name
  UNION ALL
  /* ---------- ssl_handshake_privacy ---------- */
  SELECT
    DATE(submission_timestamp) AS day,
    'ssl_handshake_privacy' AS metric,
    "" AS key,
    CASE
      WHEN COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.geo.country
      ELSE 'OTHER'
    END AS country,
    CASE
      WHEN COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.isp.name
      ELSE 'OTHER'
    END AS isp_name,
    COUNT(DISTINCT client_info.client_id) AS client_count
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`,
    UNNEST(metrics.custom_distribution.ssl_handshake_privacy.values) AS s
  WHERE
    DATE(submission_timestamp)
    BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    AND CURRENT_DATE()
    AND client_info.client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND metrics.labeled_timing_distribution.network_sup_http3_tcp_connection IS NOT NULL
  GROUP BY
    day,
    key,
    metadata.geo.country,
    metadata.isp.name
)
SELECT
  day,
  metric,
  key,
  country,
  isp_name,
  SUM(client_count) AS total_client_count
FROM
  unioned
GROUP BY
  day AS date,
  metric,
  key AS label,
  country AS coutry_code,
  isp_name
ORDER BY
  date DESC,
  metric,
  label,
  coutry_code,
  isp_name;
