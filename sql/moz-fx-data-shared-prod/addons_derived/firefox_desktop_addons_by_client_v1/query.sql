WITH base AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id,
    sample_id,
    normalized_channel,
    normalized_country_code,
    client_info.locale,
    normalized_os,
    metrics.uuid.legacy_telemetry_client_id,
    -- merging active_addons and addons_theme into a single addons object
    ARRAY_CONCAT(
      -- COALESCE to make sure array concat returns results if active_addons is empty and addons_theme is not
      COALESCE(JSON_QUERY_ARRAY(metrics.object.addons_active_addons, '$'), []),
      [
        JSON_QUERY(metrics.object.addons_theme, '$')
      ] -- wrapping the json object in array to enable array concat
    ) AS active_addons,
    -- Accepts formats: 80.0 80.0.0 80.0.0a1 80.0.0b1
    IF(
      REGEXP_CONTAINS(client_info.app_display_version, r'^(\d+\.\d+(\.\d+)?([ab]\d+)?)$'),
      client_info.app_display_version,
      NULL
    ) AS app_version,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.addons`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.client_id IS NOT NULL
    AND (
      (
        metrics.object.addons_active_addons IS NOT NULL
        AND ARRAY_LENGTH(JSON_QUERY_ARRAY(metrics.object.addons_active_addons, '$')) > 0
      )
      OR metrics.object.addons_theme IS NOT NULL
    )
),
per_client AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    normalized_channel,
    ARRAY_CONCAT_AGG(active_addons) AS active_addons,
    -- We always want to take the most recent seen version per
    -- https://bugzilla.mozilla.org/show_bug.cgi?id=1693308
    ARRAY_AGG(app_version ORDER BY mozfun.norm.truncate_version(app_version, "minor") DESC)[
      SAFE_OFFSET(0)
    ] AS app_version,
    `moz-fx-data-shared-prod.udf.mode_last`(
      ARRAY_AGG(legacy_telemetry_client_id)
    ) AS legacy_telemetry_client_id,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(normalized_country_code)) AS country,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(locale)) AS locale,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(normalized_os)) AS app_os,
  FROM
    base
  GROUP BY
    ALL
)
SELECT
  * EXCEPT (active_addons),
  ARRAY(
    SELECT AS STRUCT
      JSON_VALUE(addon, "$.id") AS id,
      -- Same methodology as for app_version above.
      ARRAY_AGG(
        JSON_VALUE(addon, "$.version")
        ORDER BY
          mozfun.norm.truncate_version(JSON_VALUE(addon, "$.version"), "minor") DESC
      )[SAFE_OFFSET(0)] AS `version`,
    FROM
      UNNEST(active_addons) AS addon
    WHERE
      addon IS NOT NULL
    GROUP BY
      id
  ) AS addons
FROM
  per_client
