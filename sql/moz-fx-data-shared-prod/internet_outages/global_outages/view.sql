CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.internet_outages.global_outages`
AS
SELECT
  country,
  city,
  geo_subdivision1,
  geo_subdivision2,
  `datetime`,
  proportion_undefined,
  proportion_timeout,
  proportion_abort,
  proportion_unreachable,
  proportion_terminated,
  proportion_channel_open,
  avg_dns_success_time,
  missing_dns_success,
  avg_dns_failure_time,
  missing_dns_failure,
  count_dns_failure,
  ssl_error_prop,
  avg_tls_handshake_time,
  "v2" AS dataset_version,
FROM
  `moz-fx-data-shared-prod.internet_outages.global_outages_v2`
WHERE
  DATE(`datetime`) >= "2024-07-26"
UNION ALL
SELECT
  country,
  city,
  CAST(NULL AS STRING) AS geo_subdivision1,
  CAST(NULL AS STRING) AS geo_subdivision2,
  `datetime`,
  proportion_undefined,
  proportion_timeout,
  proportion_abort,
  proportion_unreachable,
  proportion_terminated,
  proportion_channel_open,
  avg_dns_success_time,
  missing_dns_success,
  avg_dns_failure_time,
  missing_dns_failure,
  count_dns_failure,
  ssl_error_prop,
  avg_tls_handshake_time,
  "v1" AS dataset_version,
FROM
  `moz-fx-data-shared-prod.internet_outages.global_outages_v1`
WHERE
  DATE(`datetime`) < "2024-07-26"
