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
  submission_date = @submission_date
  -- install is successful
  AND extra_step = 'completed'
  -- only surface "listed" add-ons (add-ons listed on AMO)
  AND extra_source IN ('amo', 'disco')
  AND addon_id IS NOT NULL
GROUP BY
  submission_date,
  addon_id
