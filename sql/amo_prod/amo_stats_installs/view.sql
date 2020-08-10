CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.amo_prod.amo_stats_installs`
AS
SELECT
  submission_date,
  hashed_addon_id,
  total_downloads,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(key, 'Unknown') AS key,
      value
    FROM
      UNNEST(downloads_per_campaign)
  ) AS downloads_per_campaign,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(key, 'Unknown') AS key,
      value
    FROM
      UNNEST(downloads_per_content)
  ) AS downloads_per_content,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(key, 'Unknown') AS key,
      value
    FROM
      UNNEST(downloads_per_source)
  ) AS downloads_per_source,
  ARRAY(
    SELECT AS STRUCT
      IFNULL(key, 'Unknown') AS key,
      value
    FROM
      UNNEST(downloads_per_medium)
  ) AS downloads_per_medium,
FROM
  `moz-fx-data-shared-prod.amo_prod.amo_stats_installs_v3`
