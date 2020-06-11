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
    -- If cities are either '??' or NULL then it's from cities we either don't
    -- know about or have less than 15k people. Just rename to 'unknown'.
    IF(city = '??' OR city IS NULL, 'unknown', city) AS city,
    -- Truncate the submission timestamp to the hour.
    TIMESTAMP_TRUNC(submission_timestamp_min, HOUR) AS datetime,
    COUNT(*) AS client_count
  FROM
    telemetry.clients_daily
  WHERE
    submission_date = @submission_date
    -- Country can be null if geoip lookup failed.
    -- There's no point in adding these to the analyses.
    -- Due to a bug in `telemetry.clients_daily` we need to
    -- check for '??' as well in addition to null.
    AND country IS NOT NULL
    AND country != '??'
  GROUP BY
    1, 2, 3
  -- Filter filter out cities for which we have less than 1000 daily active
  -- users. This will make sure data won't end up in the final table.
  HAVING
    COUNT(*) > 1000
),
-- Compute aggregates for the health data.
health_data_sample AS (
  SELECT
    -- `city` is processed in `health_data_aggregates`.
    udf.geo_struct(metadata.geo.country, metadata.geo.city, NULL, NULL).* EXCEPT(geo_subdivision1, geo_subdivision2),
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS datetime,
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
    3,
    4
),
health_data_aggregates AS (
  SELECT
    country,
    -- If cities are either '??' or NULL then it's from cities we either don't
    -- know about or have less than 15k people. Just rename to 'unknown'.
    IF(city = '??' OR city IS NULL, 'unknown', city) AS city,
    datetime,
    COUNTIF(eUndefined > 0) AS num_clients_eUndefined,
    COUNTIF(eTimeOut > 0) AS num_clients_eTimeOut,
    COUNTIF(eAbort > 0) AS num_clients_eAbort,
    COUNTIF(eUnreachable > 0) AS num_clients_eUnreachable,
    COUNTIF(eTerminated > 0) AS num_clients_eTerminated,
    COUNTIF(eChannelOpen > 0) AS num_clients_eChannelOpen,
  FROM
    health_data_sample
  WHERE
    -- Country can be null if geoip lookup failed.
    -- There's no point in adding these to the analyses.
    country IS NOT NULL
  GROUP BY
    country, city, datetime
  HAVING
    COUNT(*) > 100
),
final_health_data AS (
  SELECT
    h.country,
    h.city,
    h.datetime,
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
    DAUs.datetime = h.datetime
    AND DAUs.country = h.country
    AND DAUs.city = h.city
),
-- Compute aggregates for histograms coming from the health ping.
histogram_data_sample AS (
  SELECT
    udf.geo_struct(metadata.geo.country, metadata.geo.city, NULL, NULL).country,
    -- If cities are NULL then it's from cities we either don't
    -- know about or have less than 15k people. Just rename to 'unknown'.
    IFNULL(udf.geo_struct(metadata.geo.country, metadata.geo.city, NULL, NULL).city, 'unknown') AS city,
    client_id,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS time_slot,
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
    -- Country can be null if geoip lookup failed.
    -- There's no point in adding these to the analyses.
    AND udf.geo_struct(metadata.geo.country, metadata.geo.city, NULL, NULL).country IS NOT NULL
),
-- DNS_SUCCESS histogram
dns_success_time AS (
  SELECT
    country,
    city,
    time_slot AS datetime,
    exp(sum(log(key) * count) / sum(count)) AS value
  FROM
    (
      SELECT
        country,
        city,
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
        city,
        time_slot,
        client_id,
        key
    )
  WHERE
    key > 0
  GROUP BY
    1, 2, 3
  HAVING
    COUNT(*) > 100
),
-- A shared source for the DNS_FAIL histogram
dns_failure_src AS (
  SELECT
    country,
    city,
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
    city,
    time_slot,
    client_id,
    key
),
-- DNS_FAIL histogram
dns_failure_time AS (
  SELECT
    country,
    city,
    time_slot AS datetime,
    exp(sum(log(key) * count) / sum(count)) AS value
  FROM
    dns_failure_src
  WHERE
    key > 0
  GROUP BY
    1, 2, 3
  HAVING
    COUNT(*) > 100
),
-- DNS_FAIL counts
dns_failure_counts AS (
  SELECT
    country,
    city,
    time_slot AS datetime,
    avg(count) AS value
  FROM
    (
      SELECT
        country,
        city,
        client_id,
        time_slot,
        sum(count) AS count
      FROM
        dns_failure_src
      GROUP BY
        country,
        city,
        time_slot,
        client_id
    )
  GROUP BY
    country,
    city,
    time_slot
  HAVING
    COUNT(*) > 100
),
-- TLS_HANDSHAKE histogram
tls_handshake_time AS (
  SELECT
    country,
    city,
    time_slot AS datetime,
    exp(sum(log(key) * count) / sum(count)) AS value
  FROM
    (
      SELECT
        country,
        city,
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
        city,
        time_slot,
        client_id,
        key
    )
  WHERE
    key > 0
  GROUP BY
    1, 2, 3
  HAVING
    COUNT(*) > 100
)
SELECT
  DAUs.country AS country,
  DAUs.city AS city,
  DAUs.datetime AS datetime,
  hd.* EXCEPT (datetime, country, city),
  ds.value AS avg_dns_success_time,
  df.value AS avg_dns_failure_time,
  dfc.value AS count_dns_failure,
  tls.value AS avg_tls_handshake_time
FROM
  final_health_data AS hd
-- We apply LEFT JOIN here and in the other places instead
-- of a FULL OUTER JOIN. Since LEFT is DAUs, which should contain
-- all the countries and all the days, it should always have matches
-- with whatever we pass on the RIGHT.
-- When doing a FULL OUTER JOIN, we end up sometimes with nulls on the
-- left because there are a few samples coming from telemetry.main that
-- are not accounted for in telemetry.clients_daily
LEFT JOIN
  DAUs
ON
  DAUs.datetime = hd.datetime
  AND DAUs.country = hd.country
  AND DAUs.city = hd.city
LEFT JOIN
  dns_success_time AS ds
ON
  DAUs.datetime = ds.datetime
  AND DAUs.country = ds.country
  AND DAUs.city = ds.city
LEFT JOIN
  dns_failure_time AS df
ON
  DAUs.datetime = df.datetime
  AND DAUs.country = df.country
  AND DAUs.city = df.city
LEFT JOIN
  dns_failure_counts AS dfc
ON
  DAUs.datetime = dfc.datetime
  AND DAUs.country = dfc.country
  AND DAUs.city = dfc.city
LEFT JOIN
  tls_handshake_time AS tls
ON
  DAUs.datetime = tls.datetime
  AND DAUs.country = tls.country
  AND DAUs.city = tls.city
ORDER BY
  1, 2
