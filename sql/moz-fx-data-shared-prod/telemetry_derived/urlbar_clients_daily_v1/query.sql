WITH
  combined_urlbar_picked AS (
  SELECT
    submission_date,
    client_id,
    default_search_engine,
    app_version,
    normalized_channel,
    locale,
    [ STRUCT("autofill" AS type,
      scalar_parent_urlbar_picked_autofill_sum AS index),
    STRUCT("bookmark" AS type,
      scalar_parent_urlbar_picked_bookmark_sum AS index),
    STRUCT("dynamic" AS type,
      scalar_parent_urlbar_picked_dynamic_sum AS index),
    STRUCT("extension" AS type,
      scalar_parent_urlbar_picked_extension_sum AS index),
    STRUCT("formhistory" AS type,
      scalar_parent_urlbar_picked_formhistory_sum AS index),
    STRUCT("history" AS type,
      scalar_parent_urlbar_picked_history_sum AS index),
    STRUCT("keyword" AS type,
      scalar_parent_urlbar_picked_keyword_sum AS index),
    STRUCT("remotetab" AS type,
      scalar_parent_urlbar_picked_remotetab_sum AS index),
    STRUCT("searchengine" AS type,
      scalar_parent_urlbar_picked_searchengine_sum AS index),
    STRUCT("searchsuggestion" AS type,
      scalar_parent_urlbar_picked_searchsuggestion_sum AS index),
    STRUCT("switchtab" AS type,
      scalar_parent_urlbar_picked_switchtab_sum AS index),
    STRUCT("tabtosearch" AS type,
      scalar_parent_urlbar_picked_tabtosearch_sum AS index),
    STRUCT("tip" AS type,
      scalar_parent_urlbar_picked_tip_sum AS index),
    STRUCT("topsite" AS type,
      scalar_parent_urlbar_picked_topsite_sum AS index),
    STRUCT("unknown" AS type,
      scalar_parent_urlbar_picked_unknown_sum AS index),
    STRUCT("visiturl" AS type,
      scalar_parent_urlbar_picked_visiturl_sum AS index) ] AS urlbar_picked
  FROM
   telemetry.clients_daily
  WHERE
    submission_date = @submission_date ),
  count_picked AS (
  SELECT
    submission_date,
    client_id,
    COALESCE(SUM(index.value), 0) as count_picked_total,
    mozfun.map.sum( ARRAY_AGG( STRUCT(type AS key,
          index.value AS value) ) ) AS count_picked_by_type,
    mozfun.map.sum( ARRAY_AGG(
        -- TODO: decide whether convert to 1-based index for consistency
        -- STRUCT(SAFE_CAST(index.key as INT64) + 1 AS key, index.value AS value)
        STRUCT(SAFE_CAST(index.key AS INT64) AS key,
          index.value AS value) ) ) AS count_picked_by_index
  FROM
    combined_urlbar_picked
  CROSS JOIN
    UNNEST(urlbar_picked) AS urlbar_picked
  CROSS JOIN
    UNNEST(index) AS index
  GROUP BY
    submission_date,
    client_id )
SELECT
  submission_date,
  client_id,
  default_search_engine,
  app_version,
  normalized_channel,
  locale,
  count_picked_total,
  count_picked_by_type,
  count_picked_by_index,
  urlbar_picked,
FROM
  combined_urlbar_picked
FULL OUTER JOIN
  count_picked
USING
  (submission_date,
    client_id)
