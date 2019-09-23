CREATE TEMP FUNCTION
  udf_bitmask_lowest_7() AS (0x7F);
CREATE TEMP FUNCTION
  udf_active_n_weeks_ago(x INT64, n INT64)
  RETURNS BOOLEAN
  AS (
    BIT_COUNT(x >> (7 * n) & udf_bitmask_lowest_7()) > 0
  );
CREATE TEMP FUNCTION
  udf_bitcount_lowest_7(x INT64) AS (
  	BIT_COUNT(x & udf_bitmask_lowest_7())
  );
CREATE TEMP FUNCTION
  udf_bitpos( bits INT64 ) AS ( CAST(SAFE.LOG(bits & -bits, 2) AS INT64));
CREATE TEMP FUNCTION
  udf_smoot_usage_from_bits(
    bit_arrays ARRAY<STRUCT<days_created_profile_bits INT64, days_active_bits INT64>>)
    AS ((
    WITH
      unnested AS (
      SELECT
        days_active_bits AS bits,
        udf_bitpos(days_created_profile_bits) AS dnp,
        udf_bitpos(days_active_bits) AS days_since_active,
        udf_bitcount_lowest_7(days_active_bits) AS active_days_in_week
      FROM
        UNNEST(bit_arrays) )
    SELECT AS STRUCT
      COUNTIF(bits > 0) = 0 AS is_empty_group,
      STRUCT(
        COUNTIF(days_since_active < 1) AS dau,
        COUNTIF(days_since_active < 7) AS wau,
        COUNTIF(days_since_active < 28) AS mau,
        SUM(active_days_in_week) AS active_days_in_week
      ) AS day_0,
      STRUCT(
        COUNTIF(dnp = 6) AS new_profiles
      ) AS day_6,
      STRUCT(
        COUNTIF(dnp = 13) AS new_profiles,
        COUNTIF(udf_active_n_weeks_ago(bits, 1)) AS active_in_week_0,
        COUNTIF(udf_active_n_weeks_ago(bits, 0)) AS active_in_week_1,
        COUNTIF(udf_active_n_weeks_ago(bits, 1)
          AND udf_active_n_weeks_ago(bits, 0))
          AS active_in_weeks_0_and_1,
        COUNTIF(dnp = 13 AND udf_active_n_weeks_ago(bits, 1)) AS new_profile_active_in_week_0,
        COUNTIF(dnp = 13 AND udf_active_n_weeks_ago(bits, 0)) AS new_profile_active_in_week_1,
        COUNTIF(dnp = 13 AND udf_active_n_weeks_ago(bits, 1)
          AND udf_active_n_weeks_ago(bits, 0))
          AS new_profile_active_in_weeks_0_and_1
      ) AS day_13
    FROM
      unnested ));
--
WITH
  base AS (
  SELECT
    * REPLACE(normalized_channel AS channel)
  FROM
    `moz-fx-data-derived-datasets.telemetry.clients_last_seen_raw_v1`),
  --
  nested AS (
  SELECT
    submission_date,
    [
    STRUCT('Any Firefox Desktop Activity' AS usage,
      udf_smoot_usage_from_bits(ARRAY_AGG(STRUCT(days_created_profile_bits,
        days_seen_bits))) AS metrics),
    STRUCT('Firefox Desktop Visited 5 URI' AS usage,
      udf_smoot_usage_from_bits(ARRAY_AGG(STRUCT(days_created_profile_bits,
          days_visited_5_uri_bits))) AS metrics),
    STRUCT('Firefox Desktop Opened Dev Tools' AS usage,
      udf_smoot_usage_from_bits(ARRAY_AGG(STRUCT(days_created_profile_bits,
          days_opened_dev_tools_bits))) AS metrics)
    ] AS metrics_array,
    MOD(ABS(FARM_FINGERPRINT(client_id)), 20) AS id_bucket,
    app_name,
    app_version,
    country,
    locale,
    os,
    os_version,
    channel
  FROM
    base
  WHERE
    client_id IS NOT NULL
    -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
    AND (@submission_date IS NULL OR @submission_date = submission_date)
  GROUP BY
    submission_date,
    id_bucket,
    app_name,
    app_version,
    country,
    locale,
    os,
    os_version,
    channel )
  --
SELECT
  submission_date,
  m.usage,
  (SELECT AS STRUCT m.metrics.* EXCEPT (is_empty_group)) AS metrics,
  nested.* EXCEPT (submission_date, metrics_array)
FROM
  nested
CROSS JOIN
  UNNEST(metrics_array) AS m
WHERE
  -- Optimization so we don't have to store rows where counts are all zero.
  NOT m.metrics.is_empty_group
