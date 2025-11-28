WITH stg_section_items AS (
  SELECT
    event_id,
    unstruct_event_com_pocket_object_update_1.trigger AS object_update_trigger,
  -- section item info
    contexts_com_pocket_section_item_1[
      0
    ].approved_corpus_item_external_id AS approved_corpus_item_external_id,
    contexts_com_pocket_section_item_1[0].url AS url,
    TIMESTAMP_SECONDS(contexts_com_pocket_section_item_1[0].created_at) AS created_at,
    contexts_com_pocket_section_item_1[0].deactivate_source AS source,
    contexts_com_pocket_section_item_1[0].deactivate_reasons AS reasons,
    contexts_com_pocket_section_item_1[0].section_external_id AS section_id,
    contexts_com_pocket_section_item_1[0].section_item_external_id AS section_item_id,
    contexts_com_pocket_section_item_1[0]._schema_version AS schema_version,
  -- event info
    derived_tstamp AS happened_at,
  FROM
    `moz-fx-data-shared-prod.snowplow_external.events`
  WHERE
    event_name = 'object_update'
    AND unstruct_event_com_pocket_object_update_1.object = 'section_item'
    AND app_id NOT LIKE '%-dev'
    AND app_id LIKE 'pocket-%'
    {% if not is_init() %}
      AND DATE(derived_tstamp) = @submission_date
    {% endif %}
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY dvce_created_tstamp) = 1
)
SELECT
  s.approved_corpus_item_external_id,
  s.object_update_trigger AS event_name,
  s.url,
  s.section_id,
  s.section_item_id,
  s.reasons,
  s.created_at,
  s.source,
  s.happened_at,
  s.schema_version,
  TO_BASE64(
    SHA256(CONCAT(s.section_item_id, s.object_update_trigger))
  ) AS section_item_id_object_update_trigger_key
FROM
  stg_section_items s
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      section_item_id,
      object_update_trigger
    ORDER BY
      happened_at DESC
  ) = 1;
