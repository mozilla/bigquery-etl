SELECT
  JSON_VALUE(run_details.candidate_set_id) AS candidate_set_id,
  -- line below is copied over from original DBT model
  'prospect' AS candidate_set_type, -- the hardcoding is for backward compatiblility with the legacy item feed from candidate set generation
  TIMESTAMP_SECONDS(LAX_INT64(run_details.expires_at)) AS expires_at,
  JSON_VALUE(run_details.flow) AS flow,
  happened_at,
  topic AS predicted_topic,
  prospect_id,
  prospect_source,
  LAX_INT64(features.rank) AS rank,
  JSON_VALUE(run_details.run_id) AS run_id,
  scheduled_surface_id,
  schema_version,
  happened_at AS snowflake_loaded_at
FROM
  `moz-fx-data-shared-prod.snowflake_migration_derived.prospects_v1`
WHERE
  object_update_trigger = 'prospect_created'
  {% if is_init() %}
    -- 2024-09-19 is the earliest date we have data from snowplow
    AND DATE(happened_at) >= '2024-09-19'
  {% else %}
    -- @submission_date is the default name for the query param
    -- automatically passed in when the job runs
    AND DATE(happened_at) = @submission_date
  {% endif %}
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY prospect_id ORDER BY happened_at DESC) = 1;
