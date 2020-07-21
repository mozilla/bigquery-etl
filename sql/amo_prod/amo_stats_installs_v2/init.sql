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
    udf.get_key(event_map_values, 'addon_id') AS addon_id,
    -- `step` is needed to only get one record for 1 download/install.
    udf.get_key(event_map_values, 'step') AS extra_step,
    -- We need to filter out installs not initiated from AMO (or DISCO).
    udf.get_key(event_map_values, 'source') AS extra_source,
    -- We retain `method` here to show how we could retrieve
    -- attribution data instead (this isn't collected yet).
    -- For AMO, we do NOT need this field. We'll have something
    -- like `extra_attribution` or `extra_utm_params` probably.
    udf.get_key(event_map_values, 'method') AS extra_method,
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
  ARRAY_AGG(DISTINCT extra_method IGNORE NULLS) AS extra_methods
FROM
  installs
WHERE
  submission_date = '2020-06-30'
  -- install is successful
  AND extra_step = 'completed'
  -- only surface "listed" add-ons (add-ons listed on AMO)
  AND extra_source IN ('amo', 'disco')
  AND addon_id IS NOT NULL
GROUP BY
  submission_date,
  addon_id
