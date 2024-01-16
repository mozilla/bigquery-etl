-- Note: udf.udf_json_extract_int_map map doesn't work in this case as it expects an INT -> INT
-- map, while we have a STRING->int map
CREATE TEMP FUNCTION udf_json_extract_string_to_int_map(input STRING) AS (
  ARRAY(
    SELECT
      STRUCT(
        CAST(SPLIT(entry, ':')[OFFSET(0)] AS STRING) AS key,
        CAST(SPLIT(entry, ':')[OFFSET(1)] AS INT64) AS value
      )
    FROM
      UNNEST(SPLIT(REPLACE(TRIM(input, '{}'), '"', ''), ',')) AS entry
    WHERE
      LENGTH(entry) > 0
  )
);

-- Get a stable source for DAUs.
WITH DAUs AS (
  SELECT
    submission_date AS date,
    COUNT(*) AS client_count
  FROM
    telemetry.clients_daily
  WHERE
    submission_date >= '2020-01-01'
    AND submission_date <= '2020-03-31'
    AND country = 'IT'
  GROUP BY
    1
  HAVING
    -- The minimum size of a bucket for it to be published.
    COUNT(*) > 5000
),
-- Compute aggregates for the health data.
health_data_sample AS (
  SELECT
    DATE(SAFE_CAST(creation_date AS TIMESTAMP)) AS date,
    client_id,
    SUM(
      COALESCE(
        CAST(JSON_EXTRACT(additional_properties, '$.payload.sendFailure.undefined') AS INT64),
        0
      )
    ) AS eUndefined,
    SUM(
      COALESCE(
        CAST(JSON_EXTRACT(additional_properties, '$.payload.sendFailure.timeout') AS INT64),
        0
      )
    ) AS eTimeOut,
    SUM(
      COALESCE(CAST(JSON_EXTRACT(additional_properties, '$.payload.sendFailure.abort') AS INT64), 0)
    ) AS eAbort,
    SUM(
      COALESCE(
        CAST(JSON_EXTRACT(additional_properties, '$.payload.sendFailure.eUnreachable') AS INT64),
        0
      )
    ) AS eUnreachable,
    SUM(
      COALESCE(
        CAST(JSON_EXTRACT(additional_properties, '$.payload.sendFailure.eTerminated') AS INT64),
        0
      )
    ) AS eTerminated,
    SUM(
      COALESCE(
        CAST(JSON_EXTRACT(additional_properties, '$.payload.sendFailure.eChannelOpen') AS INT64),
        0
      )
    ) AS eChannelOpen,
  FROM
    telemetry.health
  WHERE
    DATE(submission_timestamp) >= '2020-01-01'
    AND DATE(submission_timestamp) <= '2020-03-31'
    AND normalized_country_code = 'IT'
  GROUP BY
    1,
    2
),
health_data_aggregates AS (
  SELECT
    date,
    COUNTIF(eUndefined > 0) AS num_clients_eUndefined,
    COUNTIF(eTimeOut > 0) AS num_clients_eTimeOut,
    COUNTIF(eAbort > 0) AS num_clients_eAbort,
    COUNTIF(eUnreachable > 0) AS num_clients_eUnreachable,
    COUNTIF(eTerminated > 0) AS num_clients_eTerminated,
    COUNTIF(eChannelOpen > 0) AS num_clients_eChannelOpen,
  FROM
    health_data_sample
  GROUP BY
    date
  HAVING
    COUNT(*) > 5000
),
final_health_data AS (
  SELECT
    h.date,
    (num_clients_eUndefined / DAUs.client_count) AS proportion_undefined,
    (num_clients_eTimeOut / DAUs.client_count) AS proportion_timeout,
    (num_clients_eAbort / DAUs.client_count) AS proportion_abort,
    (num_clients_eUnreachable / DAUs.client_count) AS proportion_unreachable,
    (num_clients_eTerminated / DAUs.client_count) AS proportion_terminated,
    (num_clients_eChannelOpen / DAUs.client_count) AS proportion_channel_open,
  FROM
    health_data_aggregates AS h
  INNER JOIN
    DAUs
    ON DAUs.date = h.date
),
-- Compute aggregates for histograms coming from the health ping.
histogram_data_sample AS (
  SELECT
    client_id,
    DATE(SAFE_CAST(creation_date AS TIMESTAMP)) AS time_slot,
    mozfun.hist.extract(payload.histograms.dns_failed_lookup_time).values AS dns_fail,
    mozfun.hist.extract(payload.histograms.dns_lookup_time).values AS dns_success,
    mozfun.hist.extract(payload.histograms.ssl_cert_verification_errors).values AS ssl_cert_errors,
    mozfun.hist.extract(
      payload.processes.content.histograms.http_page_tls_handshake
    ).values AS tls_handshake,
  FROM
    telemetry.main
  WHERE
    DATE(submission_timestamp) >= '2020-01-01'
    AND DATE(submission_timestamp) <= '2020-03-31'
    -- Additionally limit the creation date.
    AND DATE(SAFE_CAST(creation_date AS TIMESTAMP)) >= '2020-01-01'
    AND DATE(SAFE_CAST(creation_date AS TIMESTAMP)) <= '2020-03-31'
    AND metadata.geo.country = 'IT'
    -- Restrict to Firefox.
    AND normalized_app_name = 'Firefox'
    -- Only to pings who seem to represent an active session.
    AND payload.info.subsession_length >= 0
),
-- DNS_SUCCESS histogram
dns_success_time AS (
  SELECT
    time_slot AS date,
    EXP(SUM(LOG(key) * count) / SUM(count)) AS value
  FROM
    (
      SELECT
        client_id,
        time_slot,
        key,
        SUM(value) AS count
      FROM
        histogram_data_sample
      CROSS JOIN
        UNNEST(histogram_data_sample.dns_success)
      GROUP BY
        time_slot,
        client_id,
        key
    )
  WHERE
    key > 0
  GROUP BY
    1
  HAVING
    COUNT(DISTINCT(client_id)) > 5000
),
-- A shared source for the DNS_FAIL histogram
dns_failure_src AS (
  SELECT
    client_id,
    time_slot,
    key,
    SUM(value) AS count
  FROM
    histogram_data_sample
  CROSS JOIN
    UNNEST(histogram_data_sample.dns_fail)
  GROUP BY
    time_slot,
    client_id,
    key
),
-- DNS_FAIL histogram
dns_failure_time AS (
  SELECT
    time_slot AS date,
    EXP(SUM(LOG(key) * count) / SUM(count)) AS value
  FROM
    dns_failure_src
  WHERE
    key > 0
  GROUP BY
    1
  HAVING
    COUNT(DISTINCT(client_id)) > 5000
),
-- DNS_FAIL counts
dns_failure_counts AS (
  SELECT
    time_slot AS date,
    AVG(count) AS value
  FROM
    (
      SELECT
        client_id,
        time_slot,
        SUM(count) AS count
      FROM
        dns_failure_src
      GROUP BY
        time_slot,
        client_id
    )
  GROUP BY
    time_slot
  HAVING
    COUNT(DISTINCT(client_id)) > 5000
),
-- TLS_HANDSHAKE histogram
tls_handshake_time AS (
  SELECT
    time_slot AS date,
    EXP(SUM(LOG(key) * count) / SUM(count)) AS value
  FROM
    (
      SELECT
        client_id,
        time_slot,
        key,
        SUM(value) AS count
      FROM
        histogram_data_sample
      CROSS JOIN
        UNNEST(histogram_data_sample.tls_handshake)
      GROUP BY
        time_slot,
        client_id,
        key
    )
  WHERE
    key > 0
  GROUP BY
    1
  HAVING
    COUNT(DISTINCT(client_id)) > 5000
)
SELECT
  DAUs.date AS date,
  hd.* EXCEPT (date),
  ds.value AS avg_dns_success_time,
  df.value AS avg_dns_failure_time,
  dfc.value AS count_dns_failure,
  tls.value AS avg_tls_handshake_time
FROM
  final_health_data AS hd
FULL OUTER JOIN
  DAUs
  ON DAUs.date = hd.date
FULL OUTER JOIN
  dns_success_time AS ds
  ON DAUs.date = ds.date
FULL OUTER JOIN
  dns_failure_time AS df
  ON DAUs.date = df.date
FULL OUTER JOIN
  dns_failure_counts AS dfc
  ON DAUs.date = dfc.date
FULL OUTER JOIN
  tls_handshake_time AS tls
  ON DAUs.date = tls.date
ORDER BY
  1
