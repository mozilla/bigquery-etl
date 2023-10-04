CREATE TEMP FUNCTION enumerated_array(results ARRAY<STRING>) AS (
  ARRAY(SELECT STRUCT(
    off + 1 AS position,
    r AS result_type,
    `mozfun.norm.result_type_to_product_name`(r) as product_result_type
  ) FROM UNNEST(results) AS r WITH OFFSET off)
);

CREATE TEMP FUNCTION get_event_action(event_name STRING, engagement_type STRING) AS (
 CASE
    WHEN event_name = 'engagement'
      AND (
        (
          engagement_type IN (
            "click",
            "drop_go",
            "enter",
            "go_button",
            "paste_go"
          )
        )
      )
      THEN 'engaged'
    WHEN event_name = 'abandonment'
      THEN 'abandoned'
    WHEN event_name = 'engagement'
      AND (
        (
          engagement_type NOT IN (
            "click",
            "drop_go",
            "enter",
            "go_button",
            "paste_go"
          )
        )
      )
      THEN "annoyance"
    ELSE NULL
  END
);

CREATE TEMP FUNCTION get_is_terminal(selected_result STRING, engagement_type STRING) AS (
  --events where the urlbar dropdown menu remains open (i.e., the urlbar session did not end)
  COALESCE(
    NOT (selected_result = 'tab_to_search' AND engagement_type IN ('click', 'enter', 'go_button'))
    AND NOT (
      selected_result = 'tip_dismissal_acknowledgement'
      AND engagement_type IN ('click', 'enter')
    )
    AND NOT (
      engagement_type IN (
        'dismiss',
        'inaccurate_location',
        'not_interested',
        'not_relevant',
        'show_less_frequently'
      )
    ),
    TRUE
  )
);

CREATE TEMP FUNCTION create_event_id(client_id STRING, submission_timestamp TIMESTAMP, name STRING, timestamp INTEGER) AS (
  CONCAT(client_id, "||", CAST(submission_timestamp AS STRING), "||", name, "||", CAST(timestamp AS STRING))
);

WITH events_unnested AS (
SELECT
  DATE(submission_timestamp) AS submission_date,
  client_info.client_id AS glean_client_id,
  metrics.uuid.legacy_telemetry_client_id AS legacy_telemetry_client_id,
  sample_id,
  name AS event_name,
  timestamp AS event_timestamp,
  create_event_id(client_info.client_id, submission_timestamp, name, timestamp) AS event_id,
  ping_info.experiments,
  ping_info.seq,
  normalized_channel,
  normalized_country_code,
  `moz-fx-data-shared-prod`.udf.normalize_search_engine(mozfun.map.get_key(extra, "search_engine_default_id")) AS normalized_engine,
  COALESCE(metrics.boolean.urlbar_pref_suggest_data_collection, FALSE) AS pref_data_collection,
  COALESCE(metrics.boolean.urlbar_pref_suggest_sponsored, FALSE) AS pref_sponsored_suggestions,
  COALESCE(metrics.boolean.urlbar_pref_suggest_nonsponsored, FALSE) AS pref_fx_suggestions,
  mozfun.map.get_key(extra, "engagement_type") AS engagement_type,
  CAST(mozfun.map.get_key(extra, "n_chars") AS int) AS num_chars_typed,
  CAST(mozfun.map.get_key(extra, "n_results") AS int) AS num_total_results,
  --If 0, then no result was selected.
  NULLIF(CAST(mozfun.map.get_key(extra, "selected_position") AS int), 0) AS selected_position,
  mozfun.map.get_key(extra, "selected_result") AS selected_result,
  enumerated_array(SPLIT(mozfun.map.get_key(extra, "results"), ',')) AS results,
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.events_v1`,
    UNNEST(events)
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND category = 'urlbar'
    AND name IN ('engagement', 'abandonment')
)
SELECT
  *,
  `mozfun.norm.result_type_to_product_name`(selected_result) AS product_selected_result,
  get_event_action(event_name, engagement_type) AS event_action,
  get_is_terminal(selected_result, engagement_type) AS is_terminal,
  CASE
    WHEN get_event_action(event_name, engagement_type) IN ('engaged', 'annoyance')
      THEN selected_result
    ELSE NULL
  END AS engaged_result_type,
  CASE
    WHEN get_event_action(event_name, engagement_type) IN ('engaged', 'annoyance')
      THEN `mozfun.norm.result_type_to_product_name`(selected_result)
    ELSE NULL
  END AS product_engaged_result_type,
  CASE
    WHEN  get_event_action(event_name, engagement_type) = 'annoyance'
      THEN engagement_type
    ELSE NULL
  END AS annoyance_signal_type,
FROM
  events_unnested
