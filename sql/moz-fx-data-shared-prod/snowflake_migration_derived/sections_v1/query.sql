-- Count Section/SectionItem events by trigger (and by event type)
WITH base AS (
  SELECT
    event_id,
    unstruct_event_com_pocket_object_update_1.object AS event_type,   -- 'section' | 'section_item'
    unstruct_event_com_pocket_object_update_1.trigger AS trigger,     -- e.g., section_added, section_item_removed, etc.
    dvce_created_tstamp,
    derived_tstamp
  FROM `moz-fx-data-shared-prod.snowplow_external.events`
  WHERE
    event_name = 'object_update'
    AND unstruct_event_com_pocket_object_update_1.object IN ('section', 'section_item')
    AND app_id LIKE 'pocket-%'
    AND app_id NOT LIKE '%-dev'
    AND DATE(derived_tstamp) >= '2025-11-17'   -- Only include recent data
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY dvce_created_tstamp) = 1
)

-- Overall counts by trigger
SELECT
  trigger,
  COUNT(*) AS event_count
FROM base
GROUP BY trigger
ORDER BY event_count DESC

--- breakdown by trigger
SELECT
  CONCAT(event_type, ' | ', trigger) AS trigger,
  COUNT(*) AS event_count
FROM base
GROUP BY event_type, trigger
ORDER BY event_count DESC;
