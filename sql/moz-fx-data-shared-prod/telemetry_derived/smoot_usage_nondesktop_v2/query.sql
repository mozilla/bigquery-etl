WITH base AS (
  SELECT
    *,
    normalized_channel AS channel,
    (campaign IS NOT NULL) AS attributed,
  FROM
    telemetry.nondesktop_clients_last_seen
  WHERE
    product != 'Other'
),
--
unioned AS (
  SELECT
    *
  FROM
    base
  UNION ALL
  SELECT
    * REPLACE ('Firefox Non-desktop' AS product)
  FROM
    base
  WHERE
    contributes_to_2020_kpi
  UNION ALL
  SELECT
    * REPLACE ('Preview+Fenix' AS product)
  FROM
    base
  WHERE
    product IN ('Fenix', 'Firefox Preview')
),
--
nested AS (
  SELECT
    submission_date,
    [
      STRUCT(
        FORMAT('Any %s Activity', product) AS usage,
        udf.smoot_usage_from_28_bits(
          ARRAY_AGG(STRUCT(days_created_profile_bits, days_seen_bits))
        ) AS metrics
      )
    ] AS metrics_array,
    MOD(ABS(FARM_FINGERPRINT(client_id)), 20) AS id_bucket,
    product AS app_name,
    app_version,
    country,
    locale,
    os,
    os_version,
    channel,
    attributed
  FROM
    unioned
  WHERE
    client_id IS NOT NULL
    -- Reprocess all dates by running this query with --parameter=submission_date:DATE:NULL
    AND (@submission_date IS NULL OR @submission_date = submission_date)
  GROUP BY
    submission_date,
    id_bucket,
    product,
    app_version,
    country,
    locale,
    os,
    os_version,
    channel,
    attributed
)
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
