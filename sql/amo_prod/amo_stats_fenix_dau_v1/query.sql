WITH cd AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    array_concat_agg(metrics.string_list.addons_enabled_addons) AS active_addons,
    udf.mode_last(array_agg(client_info.app_build)) AS app_version,
    udf.mode_last(array_agg(normalized_country_code)) AS country,
    udf.mode_last(array_agg(client_info.locale)) AS locale,
    udf.mode_last(array_agg(normalized_os)) AS app_os,
  FROM
    org_mozilla_fenix_nightly_stable.metrics_v1
  WHERE
    ARRAY_LENGTH(metrics.string_list.addons_enabled_addons) > 0
    AND DATE(submission_timestamp) = '2020-06-09'
    AND client_info.client_id IS NOT NULL
  GROUP BY
    client_info.client_id,
    submission_date
),
--
unnested AS (
  SELECT
    * EXCEPT (active_addons)
  FROM
    cd
  CROSS JOIN
    UNNEST(active_addons) AS addon_id
),
--
per_addon_version AS (
  SELECT
    submission_date,
    addon_id,
    array_agg(STRUCT(key, value) ORDER BY value DESC) AS dau_by_addon_version
  FROM
    (
      SELECT
        submission_date,
        addon_id,
        'unknown' AS key,
        COUNT(DISTINCT client_id) AS value
      FROM
        unnested
      GROUP BY
        submission_date,
        addon_id,
        key
    )
  GROUP BY
    submission_date,
    addon_id
),
per_app_version AS (
  SELECT
    submission_date,
    addon_id,
    array_agg(STRUCT(key, value) ORDER BY value DESC) AS dau_by_app_version
  FROM
    (
      SELECT
        submission_date,
        addon_id,
        app_version AS key,
        COUNT(DISTINCT client_id) AS value
      FROM
        unnested
      GROUP BY
        submission_date,
        addon_id,
        key
    )
  GROUP BY
    submission_date,
    addon_id
),
per_locale AS (
  SELECT
    submission_date,
    addon_id,
    array_agg(STRUCT(key, value) ORDER BY value DESC) AS dau_by_locale
  FROM
    (
      SELECT
        submission_date,
        addon_id,
        locale AS key,
        COUNT(DISTINCT client_id) AS value
      FROM
        unnested
      GROUP BY
        submission_date,
        addon_id,
        key
    )
  GROUP BY
    submission_date,
    addon_id
),
per_country AS (
  SELECT
    submission_date,
    addon_id,
    array_agg(STRUCT(key, value) ORDER BY value DESC) AS dau_by_country
  FROM
    (
      SELECT
        submission_date,
        addon_id,
        country AS key,
        COUNT(DISTINCT client_id) AS value
      FROM
        unnested
      GROUP BY
        submission_date,
        addon_id,
        key
    )
  GROUP BY
    submission_date,
    addon_id
),
per_app_os AS (
  SELECT
    submission_date,
    addon_id,
    array_agg(STRUCT(key, value) ORDER BY value DESC) AS dau_by_app_os
  FROM
    (
      SELECT
        submission_date,
        addon_id,
        app_os AS key,
        COUNT(DISTINCT client_id) AS value
      FROM
        unnested
      GROUP BY
        submission_date,
        addon_id,
        key
    )
  GROUP BY
    submission_date,
    addon_id
),
--
total_dau AS (
  SELECT
    submission_date,
    addon_id,
    COUNT(DISTINCT client_id) AS dau
  FROM
    unnested
  GROUP BY
    submission_date,
    addon_id
)
--
SELECT
  *
FROM
  total_dau
JOIN
  per_addon_version
USING
  (submission_date, addon_id)
JOIN
  per_app_version
USING
  (submission_date, addon_id)
JOIN
  per_locale
USING
  (submission_date, addon_id)
JOIN
  per_country
USING
  (submission_date, addon_id)
JOIN
  per_app_os
USING
  (submission_date, addon_id)
