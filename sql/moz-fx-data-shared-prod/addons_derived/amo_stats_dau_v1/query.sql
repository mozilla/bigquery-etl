/*

Daily user statistics to power AMO stats pages. See bug 1572873.

Each row in this table represents a particular addon on a particular day
and provides all the information needed to populate the various
"Daily Users" plots for the AMO stats dashboard.

*/
--
WITH unioned AS (
  SELECT
    * EXCEPT (normalized_channel),
    'Desktop' AS app,
  FROM
    `moz-fx-data-shared-prod.addons_derived.firefox_desktop_addons_by_client_v1`
  WHERE
    mozfun.norm.truncate_version(app_version, "major") >= 148
  UNION ALL
    BY NAME
  SELECT
    * EXCEPT (normalized_channel),
    'Fenix' AS app,
    -- legacy_telemetry_client_id only exists on desktop, making sure the selection order and column number matches for the union.
    CAST(NULL AS STRING) AS legacy_telemetry_client_id,
  FROM
    `moz-fx-data-shared-prod.addons_derived.fenix_addons_by_client_v1`
  WHERE
    mozfun.norm.truncate_version(app_version, "major") >= 148
),
unnested AS (
  SELECT
    unioned.* EXCEPT (addons),
    addon.id AS addon_id,
    addon.version AS addon_version,
  FROM
    unioned
  CROSS JOIN
    UNNEST(addons) AS addon
  WHERE
    submission_date = @submission_date
    AND addon.id IS NOT NULL
),
--
per_addon_version AS (
  SELECT
    submission_date,
    addon_id,
    ARRAY_AGG(STRUCT(`key`, `value`) ORDER BY `value` DESC) AS dau_by_addon_version
  FROM
    (
      SELECT
        submission_date,
        addon_id,
        addon_version AS `key`,
        COUNT(DISTINCT client_id) AS `value`,
      FROM
        unnested
      GROUP BY
        submission_date,
        addon_id,
        `key`
    )
  GROUP BY
    submission_date,
    addon_id
),
per_app_version AS (
  SELECT
    submission_date,
    addon_id,
    ARRAY_AGG(STRUCT(`key`, `value`) ORDER BY `value` DESC) AS dau_by_app_version
  FROM
    (
      SELECT
        submission_date,
        addon_id,
        app_version AS `key`,
        COUNT(DISTINCT client_id) AS `value`,
      FROM
        unnested
      WHERE
        app = 'Desktop'
      GROUP BY
        submission_date,
        addon_id,
        `key`
    )
  GROUP BY
    submission_date,
    addon_id
),
per_fenix_build AS (
  SELECT
    submission_date,
    addon_id,
    ARRAY_AGG(STRUCT(`key`, `value`) ORDER BY `value` DESC) AS dau_by_fenix_build
  FROM
    (
      SELECT
        submission_date,
        addon_id,
        app_version AS `key`,
        COUNT(DISTINCT client_id) AS `value`,
      FROM
        unnested
      WHERE
        app = 'Fenix'
      GROUP BY
        submission_date,
        addon_id,
        `key`
    )
  GROUP BY
    submission_date,
    addon_id
),
per_locale AS (
  SELECT
    submission_date,
    addon_id,
    ARRAY_AGG(STRUCT(`key`, `value`) ORDER BY `value` DESC) AS dau_by_locale
  FROM
    (
      SELECT
        submission_date,
        addon_id,
        locale AS `key`,
        COUNT(DISTINCT client_id) AS `value`,
      FROM
        unnested
      GROUP BY
        submission_date,
        addon_id,
        `key`
    )
  GROUP BY
    submission_date,
    addon_id
),
per_country AS (
  SELECT
    submission_date,
    addon_id,
    ARRAY_AGG(STRUCT(`key`, `value`) ORDER BY `value` DESC) AS dau_by_country
  FROM
    (
      SELECT
        submission_date,
        addon_id,
        country AS `key`,
        COUNT(DISTINCT client_id) AS `value`,
      FROM
        unnested
      GROUP BY
        submission_date,
        addon_id,
        `key`
    )
  GROUP BY
    submission_date,
    addon_id
),
per_app_os AS (
  SELECT
    submission_date,
    addon_id,
    ARRAY_AGG(STRUCT(`key`, `value`) ORDER BY `value` DESC) AS dau_by_app_os
  FROM
    (
      SELECT
        submission_date,
        addon_id,
        app_os AS `key`,
        COUNT(DISTINCT client_id) AS `value`,
      FROM
        unnested
      GROUP BY
        submission_date,
        addon_id,
        `key`
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
LEFT JOIN
  per_addon_version
  USING (submission_date, addon_id)
LEFT JOIN
  per_app_version
  USING (submission_date, addon_id)
LEFT JOIN
  per_fenix_build
  USING (submission_date, addon_id)
LEFT JOIN
  per_locale
  USING (submission_date, addon_id)
LEFT JOIN
  per_country
  USING (submission_date, addon_id)
LEFT JOIN
  per_app_os
  USING (submission_date, addon_id)
