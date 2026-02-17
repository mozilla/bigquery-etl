CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.addons.firefox_desktop_stats_installs`
AS
-- View to combine firefox_desktop install stats from both legacy and glean based data sources.
-- Glean source containing clients using app version 148 and above.
SELECT
  submission_date,
  hashed_addon_id,
  total_downloads,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(`key`, 'Unknown') AS `key`,
      `value`
    FROM
      UNNEST(downloads_per_campaign)
  ) AS downloads_per_campaign,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(`key`, 'Unknown') AS `key`,
      `value`
    FROM
      UNNEST(downloads_per_content)
  ) AS downloads_per_content,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(`key`, 'Unknown') AS `key`,
      `value`
    FROM
      UNNEST(downloads_per_source)
  ) AS downloads_per_source,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(`key`, 'Unknown') AS `key`,
      `value`
    FROM
      UNNEST(downloads_per_medium)
  ) AS downloads_per_medium,
FROM
  `moz-fx-data-shared-prod.addons_derived.firefox_desktop_stats_installs_v1`
UNION ALL
-- Legacy source containing clients using app version major below 148.
SELECT
  submission_date,
  hashed_addon_id,
  total_downloads,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(`key`, 'Unknown') AS `key`,
      `value`
    FROM
      UNNEST(downloads_per_campaign)
  ) AS downloads_per_campaign,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(`key`, 'Unknown') AS `key`,
      `value`
    FROM
      UNNEST(downloads_per_content)
  ) AS downloads_per_content,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(`key`, 'Unknown') AS `key`,
      `value`
    FROM
      UNNEST(downloads_per_source)
  ) AS downloads_per_source,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(`key`, 'Unknown') AS `key`,
      `value`
    FROM
      UNNEST(downloads_per_medium)
  ) AS downloads_per_medium,
FROM
  `moz-fx-data-shared-prod.addons_derived.amo_stats_installs_legacy_source_v1`
