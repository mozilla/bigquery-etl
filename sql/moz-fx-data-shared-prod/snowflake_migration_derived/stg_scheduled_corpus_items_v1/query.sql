WITH scheduled_events AS (
    -- filter raw events down to just those that are schedule events
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.snowplow_external.events`
  WHERE
    event_name = 'object_update'
    AND unstruct_event_com_pocket_object_update_1.object = 'scheduled_corpus_item'
    AND app_id NOT LIKE '%-dev'
    AND app_id LIKE 'pocket-%'
    -- for all runs after inital, limit scope to entries from the current day
    {% if not is_init() %}
      AND DATE(derived_tstamp) = @submission_date
    {% endif %}
),
deduped AS (
    -- dedupe review events, preferring first event
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY dvce_created_tstamp) AS n
  FROM
    scheduled_events
)
SELECT
  event_id,
  -- object update
  unstruct_event_com_pocket_object_update_1.object AS object_update_object,
  unstruct_event_com_pocket_object_update_1.trigger AS object_update_trigger,
  -- scheduled_corpus_item info
  contexts_com_pocket_scheduled_corpus_item_1[0].object_version AS object_version,
  contexts_com_pocket_scheduled_corpus_item_1[
    0
  ].scheduled_corpus_item_external_id AS scheduled_corpus_item_external_id,
  TIMESTAMP_SECONDS(
    contexts_com_pocket_scheduled_corpus_item_1[0].scheduled_at
  ) AS scheduled_corpus_item_scheduled_at,
  contexts_com_pocket_scheduled_corpus_item_1[0].url AS url,
  contexts_com_pocket_scheduled_corpus_item_1[
    0
  ].approved_corpus_item_external_id AS approved_corpus_item_external_id,
  contexts_com_pocket_scheduled_corpus_item_1[0].scheduled_surface_id AS scheduled_surface_id,
  contexts_com_pocket_scheduled_corpus_item_1[0].scheduled_surface_name AS scheduled_surface_name,
  contexts_com_pocket_scheduled_corpus_item_1[
    0
  ].scheduled_surface_iana_timezone AS scheduled_surface_iana_timezone,
  TIMESTAMP_SECONDS(
    contexts_com_pocket_scheduled_corpus_item_1[0].created_at
  ) AS scheduled_corpus_item_created_at,
  contexts_com_pocket_scheduled_corpus_item_1[0].created_by AS curator_created_by,
  TIMESTAMP_SECONDS(
    contexts_com_pocket_scheduled_corpus_item_1[0].updated_at
  ) AS scheduled_corpus_item_updated_at,
  contexts_com_pocket_scheduled_corpus_item_1[0].updated_by AS curator_updated_by,
  contexts_com_pocket_scheduled_corpus_item_1[0].generated_by AS schedule_generated_by,
  contexts_com_pocket_scheduled_corpus_item_1[
    0
  ].original_scheduled_corpus_item_external_id AS original_scheduled_corpus_item_external_id,
  contexts_com_pocket_scheduled_corpus_item_1[0].status AS scheduled_status,
  ARRAY_TO_STRING(
    contexts_com_pocket_scheduled_corpus_item_1[0].status_reasons,
    ","
  ) AS scheduled_status_reasons,
  contexts_com_pocket_scheduled_corpus_item_1[
    0
  ].status_reason_comment AS scheduled_status_reason_comment,
  contexts_com_pocket_scheduled_corpus_item_1[0].action_screen AS scheduled_action_ui_page,
  -- event info
  derived_tstamp AS happened_at,
  geo_country,
  geo_region,
  geo_region_name,
  geo_timezone,
  app_id AS tracker_app_id,
  useragent,
  br_lang,
    -- pass through any relevant contexts/entities
  TO_JSON(
    contexts_com_snowplowanalytics_snowplow_ua_parser_context_1
  ) AS contexts_com_snowplowanalytics_snowplow_ua_parser_context_1,
  TO_JSON(contexts_nl_basjes_yauaa_context_1) AS contexts_nl_basjes_yauaa_context_1,
  TO_JSON(
    contexts_com_iab_snowplow_spiders_and_robots_1
  ) AS contexts_com_iab_snowplow_spiders_and_robots_1,
  TO_JSON(contexts_com_pocket_api_user_1) AS contexts_com_pocket_api_user_1,
  TO_JSON(unstruct_event_com_pocket_object_update_1) AS unstruct_event_com_pocket_object_update_1,
  TO_BASE64(
    SHA256(CONCAT(event_id, contexts_com_pocket_scheduled_corpus_item_1[0].object_version))
  ) AS event_id_object_version_key
FROM
  deduped
WHERE
  n = 1
