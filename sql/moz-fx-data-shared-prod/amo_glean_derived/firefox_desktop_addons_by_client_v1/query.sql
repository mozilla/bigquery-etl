WITH base AS (
  SELECT
    submission_timestamp,
    client_info.client_id,
    sample_id,
    normalized_channel,
    normalized_country_code,
    client_info.locale,
    normalized_os,
    client_info.app_display_version,
    JSON_QUERY_ARRAY(metrics.object.addons_active_addons, '$') AS active_addons,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.metrics`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
    AND metrics.object.addons_active_addons IS NOT NULL
),
per_clients_without_addons AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    sample_id,
    ARRAY_AGG(
      app_display_version
      ORDER BY
        mozfun.norm.truncate_version(app_display_version, "minor") DESC
    )[SAFE_OFFSET(0)] AS app_version,
    mozfun.stats.mode_last(
      ARRAY_AGG(normalized_country_code ORDER BY submission_timestamp)
    ) AS country,
    mozfun.stats.mode_last(ARRAY_AGG(locale ORDER BY submission_timestamp)) AS locale,
    mozfun.stats.mode_last(ARRAY_AGG(normalized_os ORDER BY submission_timestamp)) AS app_os,
  FROM
    base
  GROUP BY
    ALL
),
per_clients_just_addons AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    sample_id,
    ARRAY_CONCAT_AGG(
      ARRAY(
        SELECT AS STRUCT
          JSON_VALUE(addon, '$.id') AS id,
          JSON_VALUE(addon, '$.version') AS `version`
        FROM
          UNNEST(active_addons) AS addon
      )
    ) AS addons
  FROM
    base
  GROUP BY
    ALL
),
per_client AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    addons,
    app_version,
    country,
    locale,
    app_os,
  FROM
    per_clients_without_addons
  INNER JOIN
    per_clients_just_addons
    USING (submission_date, client_id, sample_id)
)
SELECT
  * EXCEPT (addons),
  ARRAY(
    SELECT AS STRUCT
      addon.id,
      -- Same methodology as for app_version above.
      ARRAY_AGG(addon.version ORDER BY mozfun.norm.truncate_version(addon.version, "minor") DESC)[
        SAFE_OFFSET(0)
      ] AS `version`,
    FROM
      UNNEST(addons) AS addon
    GROUP BY
      addon.id
  ) AS addons
FROM
  per_client
