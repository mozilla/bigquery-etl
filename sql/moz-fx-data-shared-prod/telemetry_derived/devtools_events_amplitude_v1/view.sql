CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry_derived.devtools_events_amplitude_v1`
  OPTIONS(
    description="A view for extracting Devtools events to Amplitude. Compatible with event taxonomy from legacy Spark jobs."
  ) AS
WITH event_events AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'event' AS doc_type,
    document_id,
    client_id,
    normalized_channel,
    normalized_country_code AS country,
    environment.settings.locale AS locale,
    normalized_app_name AS app_name,
    metadata.uri.app_version AS app_version,
    normalized_os AS os,
    normalized_os_version AS os_version,
    environment.experiments AS experiments,
    sample_id,
    payload.session_id AS session_id,
    SAFE.TIMESTAMP_MILLIS(payload.process_start_timestamp) AS session_start_time,
    payload.subsession_id AS subsession_id,
    submission_timestamp AS `timestamp`,
    `moz-fx-data-shared-prod.udf.deanonymize_event`(e).*,
    event_process,
    environment.build.architecture AS env_build_arch,
    application.build_id AS application_build_id,
    environment.settings.is_default_browser,
    environment.system.is_wow64,
    environment.system.memory_mb,
    environment.profile.creation_date AS profile_creation_date,
    environment.settings.attribution.source AS attribution_source,
    metadata.geo.city
  FROM
    `moz-fx-data-shared-prod.telemetry.event`
  CROSS JOIN
    UNNEST(
      [
        STRUCT(
          "content" AS event_process,
          payload.events.content AS events
        ),
        ("dynamic", payload.events.dynamic),
        ("extension", payload.events.extension),
        ("gpu", payload.events.gpu),
        ("parent", payload.events.parent)
      ]
    )
  CROSS JOIN
    UNNEST(events) AS e
), main_events AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    'main' AS doc_type,
    document_id,
    client_id,
    normalized_channel,
    normalized_country_code AS country,
    environment.settings.locale AS locale,
    normalized_app_name AS app_name,
    metadata.uri.app_version AS app_version,
    normalized_os AS os,
    normalized_os_version AS os_version,
    environment.experiments AS experiments,
    sample_id,
    payload.info.session_id AS session_id,
    SAFE.PARSE_TIMESTAMP(
      "%FT%H:%M:%S.0%Ez",
      payload.info.session_start_date
    ) AS session_start_time,
    payload.info.subsession_id AS subsession_id,
    submission_timestamp AS `timestamp`,
    `moz-fx-data-shared-prod.udf.deanonymize_event`(e).*,
    event_process,
    environment.build.architecture AS env_build_arch,
    application.build_id AS application_build_id,
    environment.settings.is_default_browser,
    environment.system.is_wow64,
    environment.system.memory_mb,
    environment.profile.creation_date AS profile_creation_date,
    environment.settings.attribution.source AS attribution_source,
    metadata.geo.city
  FROM
    `moz-fx-data-shared-prod.telemetry.main`
  CROSS JOIN
    -- While there are more "events" fields under other process in the main ping schema,
    -- events were moved out to the event ping before those other processes were added. This is
    -- an exhaustive list of processes in which we'd expect to see events in main pings
    UNNEST(
      [
        STRUCT(
          "content" AS event_process,
          payload.processes.content.events AS events
        ),
        ("dynamic", payload.processes.dynamic.events),
        ("gpu", payload.processes.gpu.events),
        ("parent", payload.processes.parent.events)
      ]
    )
  CROSS JOIN
    UNNEST(events) AS e
), events AS (
  SELECT
    *
  FROM
    main_events
  UNION ALL
  SELECT
    *
  FROM
    event_events
), base_events AS (
  SELECT
    *,
    timestamp AS submission_timestamp,
    event_string_value AS event_value,
    UNIX_MILLIS(session_start_time) AS created
  FROM
    events
), all_events AS (
SELECT
    submission_timestamp,
    client_id AS user_id,
    (created + COALESCE(SAFE_CAST(`moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'session_id') AS INT64), 0)) AS session_id,
    CASE
        WHEN event_object = 'tools' THEN CONCAT('dt - ',  event_method)
        WHEN event_object = 'debugger' THEN CONCAT('dt_jsdebugger - ',  event_method)
        WHEN event_object = 'aboutdebugging' THEN CONCAT('dt_adbg - ',  event_method)
        WHEN event_category = 'devtools.main' THEN CONCAT('dt_', event_object, ' - ',  event_method)
    END AS event_name,
    event_timestamp AS timestamp,
    (event_timestamp + created) AS time,
    app_version,
    os AS os_name,
    os_version,
    country,
    city,
    (SELECT
      ARRAY_AGG(CONCAT('"',
        CAST(key AS STRING), '":"',
        CAST(value AS STRING), '"'))
     FROM
       UNNEST(event_map_values)) AS event_props,
    event_map_values,
    event_object,
    event_value,
    event_method,
    event_category,
    created,
    NULL AS settings,
    normalized_channel,
    env_build_arch,
    sample_id,
    application_build_id,
    app_name,
    locale,
    is_default_browser,
    is_wow64,
    memory_mb,
    profile_creation_date,
    attribution_source,
    (SELECT ARRAY_AGG(CONCAT(key,'_',value.branch)) from UNNEST(experiments)) as experiments
FROM
    base_events
WHERE 
    (doc_type IN ('main', 'event') AND app_name = 'Firefox' AND normalized_channel IN ('nightly', 'aurora'))
    AND event_category = 'devtools.main'
    AND event_method NOT IN ('edit_rule', 'tool_timer')
), all_events_with_insert_ids AS (
SELECT
  * EXCEPT (event_category, created),
  CONCAT(user_id, "-", CAST(created AS STRING), "-", SPLIT(event_name, " - ")[OFFSET(1)], "-", CAST(timestamp AS STRING), "-", event_category, "-", event_method, "-", event_object) AS insert_id,
  event_name AS event_type
FROM
  all_events
WHERE
  event_name IS NOT NULL
), extra_props AS (
SELECT
  * EXCEPT (event_map_values, event_object, event_value, event_method, event_name),
  ARRAY_CONCAT((SELECT ARRAY_AGG(CONCAT('"', CAST(key AS STRING), '":"', CAST(value AS STRING), '"')) FROM (
    SELECT 'host' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'host') AS value
    UNION ALL SELECT 'width' AS key, `moz-fx-data-shared-prod.udf.get_key`(event_map_values, 'width') AS value
    UNION ALL SELECT 'channel' AS key, normalized_channel AS value
    UNION ALL SELECT 'app_build_id' AS key, application_build_id AS value
    UNION ALL SELECT 'app_name' AS key, app_name AS value
    UNION ALL SELECT 'locale' AS key, locale AS value
    UNION ALL SELECT 'country' AS key, country AS value
    UNION ALL SELECT 'env_build_arch' AS key, env_build_arch AS value
    UNION ALL SELECT 'source' AS key, attribution_source AS value
    UNION ALL SELECT 'profile_creation_date' AS key, CAST(SAFE.DATE_FROM_UNIX_DATE(CAST(profile_creation_date AS INT64)) AS STRING) AS value
)),
(SELECT ARRAY_AGG(CONCAT('"', CAST(key AS STRING), '":', CAST(value AS STRING))) FROM (
    SELECT 'experiments' AS key, CONCAT('["', ARRAY_TO_STRING(experiments, '","'),'"]') AS value
    UNION ALL SELECT 'sample_id' AS key, CAST(sample_id AS STRING) AS value
    UNION ALL SELECT 'is_default_browser' AS key, CAST(is_default_browser AS STRING) AS value
    UNION ALL SELECT 'is_wow64' AS key, CAST(is_wow64 AS STRING) AS value
    UNION ALL SELECT 'memory_mb' AS key, CAST(memory_mb AS STRING) AS value
))
  ) AS user_props
FROM
  all_events_with_insert_ids
)

SELECT
  * EXCEPT (event_props, user_props, settings, normalized_channel, env_build_arch, sample_id, 
    application_build_id, app_name, locale, is_default_browser, is_wow64, memory_mb, profile_creation_date,
    attribution_source, experiments),
  CONCAT('{', ARRAY_TO_STRING((
   SELECT ARRAY_AGG(DISTINCT e) FROM UNNEST(ARRAY_CONCAT(event_props)) AS e
  ), ","), '}') AS event_properties,
  CONCAT('{', ARRAY_TO_STRING(user_props, ","), '}') AS user_properties
FROM extra_props
