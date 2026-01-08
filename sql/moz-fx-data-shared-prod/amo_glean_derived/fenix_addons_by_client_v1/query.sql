WITH per_client AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    client_id,
    sample_id,
    -- merging active_addons and addons_theme into a single addons object
    ARRAY_CONCAT(
      -- COALESCE to make sure array concat returns results if active_addons is empty and addons_theme is not
      COALESCE(JSON_QUERY_ARRAY(addons_active_addons, '$'), []),
      [JSON_QUERY(addons_theme, '$')] -- wrapping the json object in array to enable array concat
    ) AS active_addons,
    -- We always want to take the most recent seen version per
    -- https://bugzilla.mozilla.org/show_bug.cgi?id=1693308
    ARRAY_AGG(app_version ORDER BY mozfun.norm.truncate_version(app_version, "minor") DESC)[
      SAFE_OFFSET(0)
    ] AS app_version,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(normalized_country_code)) AS country,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(locale)) AS locale,
    `moz-fx-data-shared-prod.udf.mode_last`(ARRAY_AGG(normalized_os)) AS app_os,
  FROM
    `moz-fx-data-shared-prod.amo_glean.addons`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND app_name = "fenix"
    AND client_id IS NOT NULL
    -- TODO: do we want this filter included here like we have on desktop?
    AND (
      (
        metrics.object.addons_active_addons IS NOT NULL
        AND ARRAY_LENGTH(JSON_QUERY_ARRAY(metrics.object.addons_active_addons, '$')) > 0
      )
      OR metrics.object.addons_theme IS NOT NULL
    )
  GROUP BY
    ALL
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
