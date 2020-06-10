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
    -- Given the `telemetry.clients_daily` implementation we don't expect
    -- ?? to be in the data (https://github.com/mozilla/bigquery-etl/blob/3f1cb398fa3eb162c232480d8cfa97b8952ee658/sql/telemetry_derived/clients_daily_v6/query.sql#L127).
    -- But reality defies expectations.
    NULLIF(country, '??') AS country,
    submission_date AS date,
    COUNT(*) AS client_count
  FROM
    telemetry.clients_daily
  WHERE
    submission_date = @submission_date
  GROUP BY
    1, 2
),
-- Compute aggregates for the health data.
health_data_sample AS (
  SELECT
    normalized_country_code AS country,
    DATE(SAFE_CAST(creation_date AS TIMESTAMP)) AS date,
    client_id,
    SUM(
      coalesce(
        CAST(JSON_EXTRACT(additional_properties, '$.payload.sendFailure.undefined') AS INT64),
        0
      )
    ) AS eUndefined,
    SUM(
      coalesce(
        CAST(JSON_EXTRACT(additional_properties, '$.payload.sendFailure.timeout') AS INT64),
        0
      )
    ) AS eTimeOut,
    SUM(
      coalesce(CAST(JSON_EXTRACT(additional_properties, '$.payload.sendFailure.abort') AS INT64), 0)
    ) AS eAbort,
    SUM(
      coalesce(
        CAST(JSON_EXTRACT(additional_properties, '$.payload.sendFailure.eUnreachable') AS INT64),
        0
      )
    ) AS eUnreachable,
    SUM(
      coalesce(
        CAST(JSON_EXTRACT(additional_properties, '$.payload.sendFailure.eTerminated') AS INT64),
        0
      )
    ) AS eTerminated,
    SUM(
      coalesce(
        CAST(JSON_EXTRACT(additional_properties, '$.payload.sendFailure.eChannelOpen') AS INT64),
        0
      )
    ) AS eChannelOpen,
  FROM
    telemetry.health
  WHERE
    date(submission_timestamp) = @submission_date
  GROUP BY
    1,
    2,
    3
),
health_data_aggregates AS (
  SELECT
    country,
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
    country, date
),
final_health_data AS (
  SELECT
    h.country,
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
  ON
    DAUs.date = h.date
    AND DAUs.country = h.country
),
-- Compute aggregates for histograms coming from the health ping.
histogram_data_sample AS (
  SELECT
    udf.geo_struct(metadata.geo.country, metadata.geo.city, NULL, NULL).country,
    client_id,
    DATE(SAFE_CAST(creation_date AS TIMESTAMP)) AS time_slot,
    udf.json_extract_int_map(
      JSON_EXTRACT(payload.histograms.dns_failed_lookup_time, '$.values')
    ) AS dns_fail,
    udf.json_extract_int_map(
      JSON_EXTRACT(payload.histograms.dns_lookup_time, '$.values')
    ) AS dns_success,
    udf.json_extract_int_map(
      JSON_EXTRACT(payload.histograms.ssl_cert_verification_errors, '$.values')
    ) AS ssl_cert_errors,
    udf.json_extract_int_map(
      JSON_EXTRACT(payload.processes.content.histograms.http_page_tls_handshake, '$.values')
    ) AS tls_handshake,
  FROM
    telemetry.main
  WHERE
    DATE(submission_timestamp) = @submission_date
    -- Restrict to Firefox.
    AND normalized_app_name = 'Firefox'
    -- Only to pings who seem to represent an active session.
    AND payload.info.subsession_length >= 0
),
-- DNS_SUCCESS histogram
dns_success_time AS (
  SELECT
    country,
    time_slot AS date,
    exp(sum(log(key) * count) / sum(count)) AS value
  FROM
    (
      SELECT
        country,
        client_id,
        time_slot,
        key,
        sum(value) AS count
      FROM
        histogram_data_sample
      CROSS JOIN
        UNNEST(histogram_data_sample.dns_success)
      GROUP BY
        country,
        time_slot,
        client_id,
        key
    )
  WHERE
    key > 0
  GROUP BY
    1, 2
),
-- A shared source for the DNS_FAIL histogram
dns_failure_src AS (
  SELECT
    country,
    client_id,
    time_slot,
    key,
    sum(value) AS count
  FROM
    histogram_data_sample
  CROSS JOIN
    UNNEST(histogram_data_sample.dns_fail)
  GROUP BY
    country,
    time_slot,
    client_id,
    key
),
-- DNS_FAIL histogram
dns_failure_time AS (
  SELECT
    country,
    time_slot AS date,
    exp(sum(log(key) * count) / sum(count)) AS value
  FROM
    dns_failure_src
  WHERE
    key > 0
  GROUP BY
    1, 2
),
-- DNS_FAIL counts
dns_failure_counts AS (
  SELECT
    country,
    time_slot AS date,
    avg(count) AS value
  FROM
    (
      SELECT
        country,
        client_id,
        time_slot,
        sum(count) AS count
      FROM
        dns_failure_src
      GROUP BY
        country,
        time_slot,
        client_id
    )
  GROUP BY
    country,
    time_slot
),
-- TLS_HANDSHAKE histogram
tls_handshake_time AS (
  SELECT
    country,
    time_slot AS date,
    exp(sum(log(key) * count) / sum(count)) AS value
  FROM
    (
      SELECT
        country,
        client_id,
        time_slot,
        key,
        sum(value) AS count
      FROM
        histogram_data_sample
      CROSS JOIN
        UNNEST(histogram_data_sample.tls_handshake)
      GROUP BY
        country,
        time_slot,
        client_id,
        key
    )
  WHERE
    key > 0
  GROUP BY
    1, 2
)
SELECT
  DAUs.date AS date,
  DAUs.country AS country,
  hd.* EXCEPT (date, country),
  ds.value AS avg_dns_success_time,
  df.value AS avg_dns_failure_time,
  dfc.value AS count_dns_failure,
  tls.value AS avg_tls_handshake_time
FROM
  final_health_data AS hd
FULL OUTER JOIN
  DAUs
ON
  DAUs.date = hd.date
  AND DAUs.country = hd.country
FULL OUTER JOIN
  dns_success_time AS ds
ON
  DAUs.date = ds.date
  AND DAUs.country = ds.country
FULL OUTER JOIN
  dns_failure_time AS df
ON
  DAUs.date = df.date
  AND DAUs.country = df.country
FULL OUTER JOIN
  dns_failure_counts AS dfc
ON
  DAUs.date = dfc.date
  AND DAUs.country = dfc.country
FULL OUTER JOIN
  tls_handshake_time AS tls
ON
  DAUs.date = tls.date
  AND DAUs.country = tls.country
ORDER BY
  1
