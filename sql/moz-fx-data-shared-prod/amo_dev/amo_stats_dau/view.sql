CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.amo_dev.amo_stats_dau`
AS
SELECT
  submission_date,
  addon_id,
  dau,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(key, 'Unknown') AS key,
      value
    FROM
      UNNEST(dau_by_addon_version)
  ) AS dau_by_addon_version,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(key, 'Unknown') AS key,
      value
    FROM
      UNNEST(dau_by_app_os)
  ) AS dau_by_app_os,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(key, 'Unknown') AS key,
      value
    FROM
      UNNEST(dau_by_app_version)
  ) AS dau_by_app_version,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(key, 'Unknown') AS key,
      value
    FROM
      UNNEST(dau_by_fenix_build)
  ) AS dau_by_fenix_build,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(key, 'Unknown') AS key,
      value
    FROM
      UNNEST(dau_by_country)
  ) AS dau_by_country,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(key, 'Unknown') AS key,
      value
    FROM
      UNNEST(dau_by_locale)
  ) AS dau_by_locale,
FROM
  `moz-fx-data-shared-prod.amo_dev.amo_stats_dau_v2`
