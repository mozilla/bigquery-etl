WITH
  base AS (
  SELECT
    * REPLACE(country_names.code AS country),
    user_id AS client_id,
    days_registered_bits AS days_created_profile_bits,
    language AS locale,
    os_name AS os,
    CAST(NULL AS STRING) AS app_name,
    CAST(NULL AS STRING) AS channel,
    CAST(NULL AS BOOLEAN) AS attributed,
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_last_seen_v1` AS fxa
  LEFT JOIN
    `moz-fx-data-shared-prod.static.country_names_v1` AS country_names
    ON fxa.country = country_names.`name` ),
  --
  nested AS (
  SELECT
    submission_date,
    [
    STRUCT('Any Firefox Account Activity' AS usage,
      udf.smoot_usage_from_28_bits(ARRAY_AGG(STRUCT(days_created_profile_bits,
        days_seen_bits))) AS metrics)
    ] AS metrics_array,
    MOD(ABS(FARM_FINGERPRINT(client_id)), 20) AS id_bucket,
    app_name,
    app_version,
    country,
    locale,
    os,
    os_version,
    channel,
    attributed
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
    channel,
    attributed )
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
