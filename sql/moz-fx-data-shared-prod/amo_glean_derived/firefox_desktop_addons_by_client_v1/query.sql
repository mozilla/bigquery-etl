WITH base AS (
  SELECT
    *,
    -- merging active_addons and addons_theme into a single addons object
    ARRAY_CONCAT(
      -- COALESCE to make sure array concat returns results if active_addons is empty and addons_theme is not
      COALESCE(JSON_QUERY_ARRAY(addons_active_addons, '$'), []),
      [JSON_QUERY(addons_theme, '$')] -- wrapping the json object in array to enable array concat
    ) AS active_addons,
  FROM
    `moz-fx-data-shared-prod.amo_glean.addons`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND app_name = "firefox_desktop"
    AND client_id IS NOT NULL
    AND (
      (
        addons_active_addons IS NOT NULL
        AND ARRAY_LENGTH(JSON_QUERY_ARRAY(addons_active_addons, '$')) > 0
      )
      OR addons_theme IS NOT NULL
    )
),
per_clients_without_addons AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    mozfun.stats.mode_last(
      ARRAY_AGG(legacy_telemetry_client_id ORDER BY submission_timestamp)
    ) AS legacy_telemetry_client_id,
    ARRAY_AGG(app_version ORDER BY mozfun.norm.truncate_version(app_version, "minor") DESC)[
      SAFE_OFFSET(0)
    ] AS app_version,
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
    submission_date,
    client_id,
    sample_id,
    mozfun.stats.mode_last(
      ARRAY_AGG(legacy_telemetry_client_id ORDER BY submission_timestamp)
    ) AS legacy_telemetry_client_id,
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
    legacy_telemetry_client_id,
    addons,
    app_version,
    country,
    locale,
    app_os,
  FROM
    per_clients_without_addons
  INNER JOIN
    per_clients_just_addons
    USING (submission_date, client_id, sample_id, legacy_telemetry_client_id)
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
