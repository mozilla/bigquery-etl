WITH stg_prospects AS (
  SELECT
    event_id,
  -- object update
    unstruct_event_com_pocket_object_update_1.object AS object_update_object,
    unstruct_event_com_pocket_object_update_1.trigger AS object_update_trigger,
  -- prospect info
    contexts_com_pocket_prospect_1[0].prospect_id AS prospect_id,
    contexts_com_pocket_prospect_1[0].url AS url,
    contexts_com_pocket_prospect_1[0].scheduled_surface_id AS scheduled_surface_id,
    contexts_com_pocket_prospect_1[0].prospect_source AS prospect_source,
    FORMAT_TIMESTAMP(
      '%Y-%m-%dT%H:%M:%E*SZ',
      TIMESTAMP_SECONDS(contexts_com_pocket_prospect_1[0].created_at)
    ) AS created_at,
    FORMAT_TIMESTAMP(
      '%Y-%m-%dT%H:%M:%E*SZ',
      TIMESTAMP_SECONDS(contexts_com_pocket_prospect_1[0].reviewed_at)
    ) AS reviewed_at,
    contexts_com_pocket_prospect_1[0].prospect_review_status AS prospect_review_status,
    IFNULL(contexts_com_pocket_prospect_1[0].status_reasons, []) AS status_reasons,
    contexts_com_pocket_prospect_1[0].status_reason_comment AS status_reason_comment,
    contexts_com_pocket_prospect_1[0].reviewed_by AS reviewed_by,
    contexts_com_pocket_prospect_1[0].title AS title,
    contexts_com_pocket_prospect_1[0].excerpt AS excerpt,
    contexts_com_pocket_prospect_1[0].image_url AS image_url,
    contexts_com_pocket_prospect_1[0].language AS LANGUAGE,
    contexts_com_pocket_prospect_1[0].topic AS topic,
    contexts_com_pocket_prospect_1[0].is_collection AS is_collection,
    contexts_com_pocket_prospect_1[0].is_syndicated AS is_syndicated,
    contexts_com_pocket_prospect_1[0].authors AS authors,
    contexts_com_pocket_prospect_1[0].publisher AS publisher,
    contexts_com_pocket_prospect_1[0].domain AS domain,
    contexts_com_pocket_prospect_1[0].features AS features,
    contexts_com_pocket_prospect_1[0].run_details AS run_details,
    contexts_com_pocket_prospect_1[0]._schema_version AS schema_version,
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
    contexts_com_pocket_prospect_1,
    unstruct_event_com_pocket_object_update_1
  FROM
    `moz-fx-data-shared-prod.snowplow_external.events`
  WHERE
    event_name = 'object_update'
    AND unstruct_event_com_pocket_object_update_1.object = 'prospect'
    AND SAFE_CAST(contexts_com_pocket_prospect_1[0].created_at AS INT64) IS NOT NULL
    AND SAFE_CAST(contexts_com_pocket_prospect_1[0].created_at AS INT64)
    BETWEEN 946684800000
    AND UNIX_MILLIS(CURRENT_TIMESTAMP())
    AND SAFE_CAST(contexts_com_pocket_prospect_1[0].reviewed_at AS INT64) IS NOT NULL
    AND SAFE_CAST(contexts_com_pocket_prospect_1[0].reviewed_at AS INT64)
    BETWEEN 946684800000
    AND UNIX_MILLIS(CURRENT_TIMESTAMP())
  -- This ensures recommended_at is between Jan 1, 2000, and the current time to remain within BQ limits for dates
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY dvce_created_tstamp) = 1
)
SELECT
  p.prospect_id,
  p.object_update_trigger,
  p.url,
  p.scheduled_surface_id,
  p.prospect_source,
  p.created_at,
  p.reviewed_at,
  p.prospect_review_status,
  p.status_reasons,
  p.status_reason_comment,
  p.reviewed_by,
  p.title,
  p.excerpt,
  p.image_url,
  p.language,
  p.topic,
  p.authors,
  p.publisher,
  p.domain,
  p.is_collection,
  p.is_syndicated,
  p.happened_at,
  p.features,
  p.run_details,
  p.schema_version,
  SHA256(CONCAT(p.prospect_id, p.object_update_trigger)) AS prospect_id_object_update_trigger_key
FROM
  stg_prospects p
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY prospect_id, object_update_trigger ORDER BY happened_at DESC) = 1;
