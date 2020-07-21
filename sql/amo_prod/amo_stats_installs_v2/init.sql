CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod.amo_prod.amo_stats_installs_v2`
PARTITION BY
  submission_date
CLUSTER BY
  addon_id
AS
WITH installs AS (
    --
  SELECT
    client_id,
    submission_date,
    '' AS addon_id,
    '' AS extra_step,
    '' AS extra_source,
  FROM
    telemetry.events
  WHERE
    event_category = 'addonsManager'
    AND event_method = 'install'
)
--
SELECT
  submission_date,
  addon_id,
  count(*) AS total_downloads,
  [STRUCT('unknown' AS key, count(*) AS value)] AS downloads_per_source,
  [STRUCT('unknown' AS key, count(*) AS value)] AS downloads_per_medium,
  [STRUCT('unknown' AS key, count(*) AS value)] AS downloads_per_content,
  [STRUCT('unknown' AS key, count(*) AS value)] AS downloads_per_campaign,
FROM
  installs
WHERE
  FALSE
  AND submission_date = '2010-01-01'
GROUP BY
  submission_date,
  addon_id
