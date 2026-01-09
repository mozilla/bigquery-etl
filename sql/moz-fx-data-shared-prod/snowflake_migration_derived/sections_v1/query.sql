WITH stg_section_items AS (
  SELECT
    event_id,
    unstruct_event_com_pocket_object_update_1.trigger AS object_update_trigger,
  -- section info
    TIMESTAMP_SECONDS(contexts_com_pocket_section_1[0].created_at) AS created_at,
    TIMESTAMP_SECONDS(contexts_com_pocket_section_1[0].updated_at) AS updated_at,
    contexts_com_pocket_section_1[0].deactivate_source AS source,
    TIMESTAMP_SECONDS(contexts_com_pocket_section_1[0].deactivated_at) AS deactivated_at,
    contexts_com_pocket_section_1[0].disabled AS disabled,
    contexts_com_pocket_section_1[0].description AS description,
    contexts_com_pocket_section_1[0].scheduled_surface_id AS scheduled_surface_id,
    contexts_com_pocket_section_1[0].scheduled_surface_name AS scheduled_surface_name,
    contexts_com_pocket_section_1[0].section_external_id AS section_id,
    contexts_com_pocket_section_1[0].title AS title,
    contexts_com_pocket_section_1[0].updated_by AS updated_by,
    contexts_com_pocket_section_1[0].start_date AS start_date,
    contexts_com_pocket_section_1[0]._schema_version AS schema_version,
  -- event info
    derived_tstamp AS happened_at,
  FROM
    `moz-fx-data-shared-prod.snowplow_external.events`
  WHERE
    event_name = 'object_update'
    AND unstruct_event_com_pocket_object_update_1.object = 'section'
    AND app_id NOT LIKE '%-dev'
    AND app_id LIKE 'pocket-%'
    {% if not is_init() %}
      AND DATE(derived_tstamp) = @submission_date
    {% endif %}
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY dvce_created_tstamp) = 1
)
SELECT
  s.object_update_trigger AS event_name,
  s.section_id,
  s.source,
  s.deactivated_at,
  s.disabled,
  s.title,
  s.description,
  s.scheduled_surface_id,
  s.scheduled_surface_name,
  s.updated_at,
  s.updated_by,
  s.start_date,
  s.created_at,
  s.happened_at,
  s.schema_version,
  TO_BASE64(
    SHA256(CONCAT(s.section_id, s.object_update_trigger))
  ) AS section_item_id_object_update_trigger_key
FROM
  stg_section_items AS s
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY section_id, object_update_trigger ORDER BY happened_at DESC) = 1
