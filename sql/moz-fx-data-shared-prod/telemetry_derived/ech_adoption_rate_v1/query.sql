WITH unioned AS (
  /* ---------- http3_ech_outcome ---------- */
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'http3_ech_outcome' AS metric,
    h.key AS label,
    v.key AS key,
    COUNT(DISTINCT client_info.client_id) AS client_count,
    SUM(v.value) AS handshakes,
    CASE
      WHEN SUM(v.value) >= 5000
        OR COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.geo.country
      ELSE 'OTHER'
    END AS country_code,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`,
    UNNEST(metrics.labeled_custom_distribution.http3_ech_outcome) AS h,
    UNNEST(h.value.values) AS v
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND metrics.labeled_custom_distribution.http3_ech_outcome IS NOT NULL
  GROUP BY
    submission_date,
    label,
    key,
    metadata.geo.country
  UNION ALL
  /* ---------- ssl_handshake_result_ech ---------- */
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'ssl_handshake_result_ech' AS metric,
    "" AS label,
    s.key AS key,
    COUNT(DISTINCT client_info.client_id) AS client_count,
    SUM(value) AS handshakes,
    CASE
      WHEN SUM(value) >= 5000
        OR COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.geo.country
      ELSE 'OTHER'
    END AS country_code,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`,
    UNNEST(metrics.custom_distribution.ssl_handshake_result_ech.values) AS s
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND metrics.custom_distribution.ssl_handshake_result_ech.values IS NOT NULL
  GROUP BY
    submission_date,
    label,
    key,
    metadata.geo.country
  UNION ALL
  /* ---------- ssl_handshake_result_ech_grease ---------- */
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'ssl_handshake_result_ech_grease' AS metric,
    "" AS label,
    s.key AS key,
    COUNT(DISTINCT client_info.client_id) AS client_count,
    SUM(value) AS handshakes,
    CASE
      WHEN SUM(value) >= 5000
        OR COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.geo.country
      ELSE 'OTHER'
    END AS country_code,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`,
    UNNEST(metrics.custom_distribution.ssl_handshake_result_ech_grease.values) AS s
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND metrics.custom_distribution.ssl_handshake_result_ech_grease.values IS NOT NULL
  GROUP BY
    submission_date,
    label,
    key,
    metadata.geo.country
  UNION ALL
  /* ---------- ssl_handshake_privacy ---------- */
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'ssl_handshake_privacy' AS metric,
    "" AS label,
    s.key AS key,
    COUNT(DISTINCT client_info.client_id) AS client_count,
    SUM(value) AS handshakes,
    CASE
      WHEN SUM(value) >= 5000
        OR COUNT(DISTINCT client_info.client_id) >= 5000
        THEN metadata.geo.country
      ELSE 'OTHER'
    END AS country_code,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`,
    UNNEST(metrics.custom_distribution.ssl_handshake_privacy.values) AS s
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
    AND client_info.app_channel = 'release'
    AND metrics.custom_distribution.ssl_handshake_privacy.values IS NOT NULL
  GROUP BY
    submission_date,
    label,
    key,
    metadata.geo.country
)
SELECT
  submission_date,
  metric,
  label,
  key,
  SUM(handshakes) AS handshakes,
  SUM(client_count) AS total_client_count,
  country_code
FROM
  unioned
GROUP BY
  submission_date,
  metric,
  label,
  key,
  country_code
