-- This sums the values reported by an histogram.
CREATE TEMP FUNCTION sum_values(x ARRAY<STRUCT<key INT64, value INT64>>) AS (
  (
    WITH a AS (
      SELECT
        IF(ARRAY_LENGTH(x) > 0, 1, 0) AS isPres1
    ),
    b AS (
      SELECT
        -- Truncate pathological values that are beyond the documented limits per
        -- https://firefox-source-docs.mozilla.org/toolkit/components/telemetry/collection/histograms.html#histogram-values
        SUM(LEAST(2147483648, value)) AS t
      FROM
        UNNEST(x)
      WHERE
        key > 0
    )
    SELECT
      COALESCE(isPres1 * t, 0)
    FROM
      a,
      b
  )
);

-- This counts how many times an histogram is not present.
-- It checks if the histogram is present at all and whether or not it recorded
-- any non-zero value.
CREATE TEMP FUNCTION empty(x ARRAY<STRUCT<key INT64, value INT64>>) AS (
  (
    WITH a AS (
      SELECT
        IF(ARRAY_LENGTH(x) = 0, 1, 0) AS isEmpty1
    ),
    b AS (
      SELECT
        IF(MAX(value) = 0, 1, 0) isEmpty2
      FROM
        UNNEST(x)
    ),
    c AS (
      SELECT
        IF(isEmpty2 = 1 OR isEmpty1 = 1, 1, 0) AS Empty
      FROM
        a,
        b
    )
    SELECT
      Empty
    FROM
      C
  )
);

-- Get a stable source for DAUs.
WITH DAUs AS (
  SELECT
    `moz-fx-data-shared-prod.udf.geo_struct_set_defaults`(
      country,
      city,
      geo_subdivision1,
      geo_subdivision2
    ).*,
    -- Truncate the submission timestamp to the hour. Note that this filed was
    -- introduced on the 16th December 2019, so it will be `null` for queries
    -- before that day. See https://github.com/mozilla/bigquery-etl/pull/603 .
    TIMESTAMP_TRUNC(submission_timestamp_min, HOUR) AS `datetime`,
    COUNT(*) AS client_count
  FROM
    `moz-fx-data-shared-prod.telemetry.clients_daily`
  WHERE
    submission_date = @submission_date
    -- Country can be null or `??` if geoip lookup failed.
    -- There's no point in adding these to the analyses.
    -- Due to a bug in `telemetry.clients_daily` we need to
    -- check for '??' as well in addition to null.
  GROUP BY
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    `datetime`
  -- Filter filter out cities for which we have less than or equal to
  -- 50 hourly active users. This will make sure data won't end up in
  -- the final table.
  HAVING
    country <> "??"
    AND client_count > 50
),
-- Compute aggregates for the health data.
health_data_sample AS (
  SELECT
    `moz-fx-data-shared-prod.udf.geo_struct_set_defaults`(
      metadata.geo.country,
      metadata.geo.city,
      metadata.geo.subdivision1,  -- returned field gets renamed to geo_subdivision1
      metadata.geo.subdivision2  -- returned field gets renamed to geo_subdivision2
    ).*,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS `datetime`,
    client_id,
    SUM(
      COALESCE(
        SAFE_CAST(JSON_EXTRACT(additional_properties, '$.payload.sendFailure.undefined') AS INT64),
        0
      )
    ) AS e_undefined,
    SUM(
      COALESCE(
        SAFE_CAST(JSON_EXTRACT(additional_properties, '$.payload.sendFailure.timeout') AS INT64),
        0
      )
    ) AS e_timeout,
    SUM(
      COALESCE(
        SAFE_CAST(JSON_EXTRACT(additional_properties, '$.payload.sendFailure.abort') AS INT64),
        0
      )
    ) AS e_abort,
    SUM(
      COALESCE(
        SAFE_CAST(
          JSON_EXTRACT(additional_properties, '$.payload.sendFailure.eUnreachable') AS INT64
        ),
        0
      )
    ) AS e_unreachable,
    SUM(
      COALESCE(
        SAFE_CAST(
          JSON_EXTRACT(additional_properties, '$.payload.sendFailure.eTerminated') AS INT64
        ),
        0
      )
    ) AS e_terminated,
    SUM(
      COALESCE(
        SAFE_CAST(
          JSON_EXTRACT(additional_properties, '$.payload.sendFailure.eChannelOpen') AS INT64
        ),
        0
      )
    ) AS e_channel_open,
  FROM
    `moz-fx-data-shared-prod.telemetry.health`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    `datetime`,
    client_id
  HAVING
    country <> "??"
),
health_data_aggregates AS (
  SELECT
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    `datetime`,
    COUNTIF(e_undefined > 0) AS num_clients_e_undefined,
    COUNTIF(e_timeout > 0) AS num_clients_e_timeout,
    COUNTIF(e_abort > 0) AS num_clients_e_abort,
    COUNTIF(e_unreachable > 0) AS num_clients_e_unreachable,
    COUNTIF(e_terminated > 0) AS num_clients_e_terminated,
    COUNTIF(e_channel_open > 0) AS num_clients_e_channel_open,
  FROM
    health_data_sample
  GROUP BY
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    `datetime`
  HAVING
    COUNT(*) > 50
),
final_health_data AS (
  SELECT
    h.country,
    h.city,
    h.geo_subdivision1,
    h.geo_subdivision2,
    h.datetime,
    (num_clients_e_undefined / DAUs.client_count) AS proportion_undefined,
    (num_clients_e_timeout / DAUs.client_count) AS proportion_timeout,
    (num_clients_e_abort / DAUs.client_count) AS proportion_abort,
    (num_clients_e_unreachable / DAUs.client_count) AS proportion_unreachable,
    (num_clients_e_terminated / DAUs.client_count) AS proportion_terminated,
    (num_clients_e_channel_open / DAUs.client_count) AS proportion_channel_open,
  FROM
    health_data_aggregates AS h
  INNER JOIN
    DAUs
    USING (`datetime`, country, city, geo_subdivision1, geo_subdivision2)
),
-- Compute aggregates for histograms coming from the health ping.
histogram_data_sample AS (
  SELECT
    `moz-fx-data-shared-prod.udf.geo_struct_set_defaults`(
      metadata.geo.country,
      metadata.geo.city,
      metadata.geo.subdivision1,  -- returned field gets renamed to geo_subdivision1
      metadata.geo.subdivision2  -- returned field gets renamed to geo_subdivision2
    ).*,
    client_id,
    document_id,
    TIMESTAMP_TRUNC(submission_timestamp, HOUR) AS time_slot,
    payload.info.subsession_length AS subsession_length,
    mozfun.hist.extract(payload.histograms.dns_failed_lookup_time).values AS dns_fail,
    mozfun.hist.extract(payload.histograms.dns_lookup_time).values AS dns_success,
    mozfun.hist.extract(payload.histograms.ssl_cert_verification_errors).values AS ssl_cert_errors,
    mozfun.hist.extract(
      payload.processes.content.histograms.http_page_tls_handshake
    ).values AS tls_handshake,
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.main_v5`
  WHERE
    DATE(submission_timestamp) = @submission_date
    -- Restrict to Firefox.
    AND normalized_app_name = 'Firefox'
    -- Only to pings who seem to represent an active session.
    AND payload.info.subsession_length >= 0
    -- Country can be null if geoip lookup failed.
    -- There's no point in adding these to the analyses.
    AND metadata.geo.country IS NOT NULL
),
-- DNS_SUCCESS histogram
dns_success_time AS (
  SELECT
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    time_slot AS `datetime`,
    EXP(SUM(LOG(key) * count) / SUM(count)) AS value
  FROM
    (
      SELECT
        country,
        city,
        geo_subdivision1,
        geo_subdivision2,
        client_id,
        time_slot,
        key,
        SUM(LEAST(2147483648, value)) AS count
      FROM
        histogram_data_sample
      CROSS JOIN
        UNNEST(histogram_data_sample.dns_success)
      GROUP BY
        country,
        city,
        geo_subdivision1,
        geo_subdivision2,
        time_slot,
        client_id,
        key
    )
  WHERE
    key > 0
  GROUP BY
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    `datetime`
  HAVING
    COUNT(*) > 50
),
-- Oddness: active sessions without DNS_LOOKUP_TIME
dns_no_dns_lookup_time AS (
  SELECT
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    time_slot AS `datetime`,
    SUM(IF(subsession_length > 0 AND is_empty = 1, 1, 0)) / (
      1 + SUM(IF(subsession_length > 0, 1, 0))
    ) AS value
  FROM
    (
      SELECT
        country,
        city,
        geo_subdivision1,
        geo_subdivision2,
        client_id,
        time_slot,
        subsession_length,
        empty(dns_success) AS is_empty
      FROM
        histogram_data_sample
    )
  GROUP BY
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    `datetime`
  HAVING
    COUNT(*) > 50
),
-- A shared source for the DNS_FAIL histogram
dns_failure_src AS (
  SELECT
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    client_id,
    time_slot,
    key,
    SUM(LEAST(2147483648, value)) AS count
  FROM
    histogram_data_sample
  CROSS JOIN
    UNNEST(histogram_data_sample.dns_fail)
  GROUP BY
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    time_slot,
    client_id,
    key
),
-- DNS_FAIL histogram
dns_failure_time AS (
  SELECT
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    time_slot AS `datetime`,
    EXP(SUM(LOG(key) * count) / SUM(count)) AS value
  FROM
    dns_failure_src
  WHERE
    key > 0
  GROUP BY
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    `datetime`
  HAVING
    COUNT(*) > 50
),
-- DNS_FAIL counts
dns_failure_counts AS (
  SELECT
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    time_slot AS `datetime`,
    AVG(count) AS value
  FROM
    (
      SELECT
        country,
        city,
        geo_subdivision1,
        geo_subdivision2,
        client_id,
        time_slot,
        SUM(count) AS count
      FROM
        dns_failure_src
      GROUP BY
        country,
        city,
        geo_subdivision1,
        geo_subdivision2,
        time_slot,
        client_id
    )
  GROUP BY
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    `datetime`
  HAVING
    COUNT(*) > 50
),
-- Oddness: active sessions without DNS_FAILED_LOOKUP_TIME
dns_no_dns_failure_time AS (
  SELECT
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    time_slot AS `datetime`,
    SUM(IF(subsession_length > 0 AND is_empty = 1, 1, 0)) / (
      1 + SUM(IF(subsession_length > 0, 1, 0))
    ) AS value
  FROM
    (
      SELECT
        country,
        city,
        geo_subdivision1,
        geo_subdivision2,
        client_id,
        time_slot,
        subsession_length,
        empty(dns_fail) AS is_empty
      FROM
        histogram_data_sample
    )
  GROUP BY
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    `datetime`
  HAVING
    COUNT(*) > 50
),
-- SSL_CERT_VERIFICATION_ERRORS histograms
ssl_error_prop_src AS (
  SELECT
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    time_slot,
    client_id,
    document_id,
    subsession_length,
    sum_values(ssl_cert_errors) AS ssl_sum_vals
  FROM
    histogram_data_sample
),
ssl_error_prop AS (
  SELECT
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    time_slot AS `datetime`,
    SUM(IF(subsession_length > 0 AND ssl_sum_vals > 0, 1, 0)) / (
      1 + SUM(IF(subsession_length > 0, 1, 0))
    ) AS value
  FROM
    ssl_error_prop_src
  GROUP BY
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    `datetime`
  HAVING
    COUNT(*) > 50
),
-- TLS_HANDSHAKE histogram
tls_handshake_time AS (
  SELECT
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    time_slot AS `datetime`,
    EXP(SUM(LOG(key) * count) / SUM(count)) AS value
  FROM
    (
      SELECT
        country,
        city,
        client_id,
        geo_subdivision1,
        geo_subdivision2,
        time_slot,
        key,
        SUM(LEAST(2147483648, value)) AS count
      FROM
        histogram_data_sample
      CROSS JOIN
        UNNEST(histogram_data_sample.tls_handshake)
      GROUP BY
        country,
        city,
        geo_subdivision1,
        geo_subdivision2,
        time_slot,
        client_id,
        key
    )
  WHERE
    key > 0
  GROUP BY
    country,
    city,
    geo_subdivision1,
    geo_subdivision2,
    `datetime`
  HAVING
    COUNT(*) > 50
)
SELECT
  DAUs.country AS country,
  -- Caveat: mindmind location mapping returns `??` in cases where
  -- the population is less than 15k. As long as the entry has a higher level
  -- grouping (country) we still want to keep it. For backwards compatibility,
  -- we rename this value to `unknown` to represent a lack of value.
  IF(DAUs.city = "??", "unknown", DAUs.city) AS city,
  NULLIF(DAUs.geo_subdivision1, "??") AS geo_subdivision1,
  NULLIF(DAUs.geo_subdivision2, "??") AS geo_subdivision2,
  DAUs.datetime AS `datetime`,
  hd.* EXCEPT (`datetime`, country, city, geo_subdivision1, geo_subdivision2),
  ds.value AS avg_dns_success_time,
  ds_missing.value AS missing_dns_success,
  df.value AS avg_dns_failure_time,
  df_missing.value AS missing_dns_failure,
  dfc.value AS count_dns_failure,
  ssl.value AS ssl_error_prop,
  tls.value AS avg_tls_handshake_time
FROM
  final_health_data AS hd
-- We apply LEFT JOIN here and in the other places instead
-- of a FULL OUTER JOIN. Since LEFT is DAUs, which should contain
-- all the countries and all the days, it should always have matches
-- with whatever we pass on the RIGHT.
-- When doing a FULL OUTER JOIN, we end up sometimes with nulls on the
-- left because there are a few samples coming from telemetry.main that
-- are not accounted for in `moz-fx-data-shared-prod.telemetry.clients_daily`
LEFT JOIN
  DAUs
  USING (`datetime`, country, city, geo_subdivision1, geo_subdivision2)
LEFT JOIN
  dns_success_time AS ds
  USING (`datetime`, country, city, geo_subdivision1, geo_subdivision2)
LEFT JOIN
  dns_no_dns_lookup_time AS ds_missing
  USING (`datetime`, country, city, geo_subdivision1, geo_subdivision2)
LEFT JOIN
  dns_failure_time AS df
  USING (`datetime`, country, city, geo_subdivision1, geo_subdivision2)
LEFT JOIN
  dns_failure_counts AS dfc
  USING (`datetime`, country, city, geo_subdivision1, geo_subdivision2)
LEFT JOIN
  dns_no_dns_failure_time AS df_missing
  USING (`datetime`, country, city, geo_subdivision1, geo_subdivision2)
LEFT JOIN
  tls_handshake_time AS tls
  USING (`datetime`, country, city, geo_subdivision1, geo_subdivision2)
LEFT JOIN
  ssl_error_prop AS ssl
  USING (`datetime`, country, city, geo_subdivision1, geo_subdivision2)
