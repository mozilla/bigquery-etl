/*

Daily user statistics to power AMO stats pages. See bug 1572873.

Each row in this table represents a particular addon on a particular day
and provides all the information needed to populate the various
"Daily Users" plots for the AMO stats dashboard.

*/
--
WITH cd AS (
  SELECT
    submission_date,
    client_id,
    active_addons,
    app_version,
    country,
    locale,
    os
  FROM
    telemetry.clients_daily
  WHERE
    ARRAY_LENGTH(active_addons) > 0
    AND submission_date = @submission_date
),
--
unnested AS (
  SELECT
    * EXCEPT (active_addons, version, os),
    version AS addon_version,
    os AS app_os,
  FROM
    cd
  CROSS JOIN
    UNNEST(active_addons) AS addon
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
        addon_version AS key,
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
