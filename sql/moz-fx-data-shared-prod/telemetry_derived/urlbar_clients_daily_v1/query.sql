CREATE TEMP FUNCTION one_index(x ANY TYPE) AS (
  IF(SAFE_CAST(x AS INT64) < 0, SAFE_CAST(x AS INT64), SAFE_CAST(x AS INT64) + 1)
);

CREATE TEMP FUNCTION one_index_struct(record STRUCT<k STRING, v INT64>) AS (
  STRUCT(one_index(record.k) AS key, record.v AS value)
);

CREATE TEMP FUNCTION one_index_array(map ARRAY<STRUCT<k STRING, v INT64>>) AS (
  ARRAY(SELECT one_index_struct(record) FROM UNNEST(map) AS record)
);

CREATE TEMP FUNCTION filter_probe_counts_with_outlier_values(
  map ARRAY<STRUCT<k STRING, v INT64>>
) AS (
  -- Similar to search probes/metrics, count values over 10000 are considered too high
  ARRAY(
    SELECT
      STRUCT(record.k AS key, record.v AS value)
    FROM
      UNNEST(map) AS record
    WHERE
      record.v <= 10000
  )
);

CREATE TEMP FUNCTION transform_scalar_metric_sum_columns(map ARRAY<STRUCT<k STRING, v INT64>>) AS (
    -- Transforms the sums of probe counts columns from clients_daily by:
        -- Applying 1-indexing to reflex index -> position (of urlbar choice)
        -- Replacing outlier sum values above a certain threshold with 0
  one_index_array(filter_probe_counts_with_outlier_values(map))
);

WITH combined_urlbar_picked AS (
  SELECT
    submission_date,
    client_id,
    default_search_engine,
    experiments,
    app_version,
    normalized_channel,
    IFNULL(country, '??') AS country,
    locale,
    user_pref_browser_search_region AS search_region,
    SAFE_CAST(user_pref_browser_search_suggest_enabled AS BOOL) AS suggest_enabled,
    SAFE_CAST(user_pref_browser_widget_in_navbar AS BOOL) AS in_navbar,
    SAFE_CAST(user_pref_browser_urlbar_suggest_searches AS BOOL) AS suggest_searches,
    SAFE_CAST(
      user_pref_browser_urlbar_show_search_suggestions_first AS BOOL
    ) AS show_search_suggestions_first,
    SAFE_CAST(user_pref_browser_urlbar_suggest_quicksuggest AS BOOL) AS suggest_quicksuggest,
    SAFE_CAST(
      user_pref_browser_urlbar_suggest_quicksuggest_nonsponsored AS BOOL
    ) AS suggest_quicksuggest_nonsponsored,
    SAFE_CAST(
      user_pref_browser_urlbar_suggest_quicksuggest_sponsored AS BOOL
    ) AS suggest_quicksuggest_sponsored,
    user_pref_browser_urlbar_quicksuggest_onboarding_dialog_choice AS quicksuggest_onboarding_dialog_choice,
    user_pref_browser_urlbar_quicksuggest_data_collection_enabled AS quicksuggest_data_collection_enabled,
    [
      STRUCT(
        "autofill" AS type,
        transform_scalar_metric_sum_columns(scalar_parent_urlbar_picked_autofill_sum) AS position
      ),
      STRUCT(
        "bookmark" AS type,
        transform_scalar_metric_sum_columns(scalar_parent_urlbar_picked_bookmark_sum) AS position
      ),
      STRUCT(
        "dynamic" AS type,
        transform_scalar_metric_sum_columns(scalar_parent_urlbar_picked_dynamic_sum) AS position
      ),
      STRUCT(
        "extension" AS type,
        transform_scalar_metric_sum_columns(scalar_parent_urlbar_picked_extension_sum) AS position
      ),
      STRUCT(
        "formhistory" AS type,
        transform_scalar_metric_sum_columns(scalar_parent_urlbar_picked_formhistory_sum) AS position
      ),
      STRUCT(
        "history" AS type,
        transform_scalar_metric_sum_columns(scalar_parent_urlbar_picked_history_sum) AS position
      ),
      STRUCT(
        "keyword" AS type,
        transform_scalar_metric_sum_columns(scalar_parent_urlbar_picked_keyword_sum) AS position
      ),
      STRUCT(
        "remotetab" AS type,
        transform_scalar_metric_sum_columns(scalar_parent_urlbar_picked_remotetab_sum) AS position
      ),
      STRUCT(
        "searchengine" AS type,
        transform_scalar_metric_sum_columns(
          scalar_parent_urlbar_picked_searchengine_sum
        ) AS position
      ),
      STRUCT(
        "searchsuggestion" AS type,
        transform_scalar_metric_sum_columns(
          scalar_parent_urlbar_picked_searchsuggestion_sum
        ) AS position
      ),
      STRUCT(
        "switchtab" AS type,
        transform_scalar_metric_sum_columns(scalar_parent_urlbar_picked_switchtab_sum) AS position
      ),
      STRUCT(
        "tabtosearch" AS type,
        transform_scalar_metric_sum_columns(scalar_parent_urlbar_picked_tabtosearch_sum) AS position
      ),
      STRUCT(
        "tip" AS type,
        transform_scalar_metric_sum_columns(scalar_parent_urlbar_picked_tip_sum) AS position
      ),
      STRUCT(
        "topsite" AS type,
        transform_scalar_metric_sum_columns(scalar_parent_urlbar_picked_topsite_sum) AS position
      ),
      STRUCT(
        "unknown" AS type,
        transform_scalar_metric_sum_columns(scalar_parent_urlbar_picked_unknown_sum) AS position
      ),
      STRUCT(
        "visiturl" AS type,
        transform_scalar_metric_sum_columns(scalar_parent_urlbar_picked_visiturl_sum) AS position
      )
    ] AS urlbar_picked_by_type_by_position
  FROM
    `telemetry_derived.clients_daily_joined_v1`
  WHERE
    submission_date = @submission_date
),
count_picked AS (
  SELECT
    submission_date,
    client_id,
    COALESCE(SUM(position.value), 0) AS count_picked_total,
    mozfun.map.sum(ARRAY_AGG(STRUCT(type AS key, position.value AS value))) AS count_picked_by_type,
    mozfun.map.sum(
      ARRAY_AGG(STRUCT(position.key AS key, position.value AS value))
    ) AS count_picked_by_position
  FROM
    combined_urlbar_picked
  CROSS JOIN
    UNNEST(urlbar_picked_by_type_by_position) AS urlbar_picked
  CROSS JOIN
    UNNEST(position) AS position
  GROUP BY
    submission_date,
    client_id
)
SELECT
  submission_date,
  client_id,
  default_search_engine,
  experiments,
  app_version,
  normalized_channel,
  country,
  locale,
  search_region,
  suggest_enabled,
  in_navbar,
  suggest_searches,
  show_search_suggestions_first,
  suggest_quicksuggest,
  suggest_quicksuggest_nonsponsored,
  suggest_quicksuggest_sponsored,
  quicksuggest_onboarding_dialog_choice,
  quicksuggest_data_collection_enabled,
  count_picked_total,
  count_picked_by_type,
  count_picked_by_position,
  urlbar_picked_by_type_by_position,
FROM
  combined_urlbar_picked
FULL OUTER JOIN
  count_picked
  USING (submission_date, client_id)
