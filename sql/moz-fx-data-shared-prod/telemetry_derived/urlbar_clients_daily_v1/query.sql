WITH
  temp AS (
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
      scalar_parent_urlbar_picked_visiturl_sum AS index) ] AS urlbar_picked,
    [ STRUCT("bookmarkmenu" AS type,
      scalar_parent_urlbar_searchmode_bookmarkmenu_sum AS index),
    STRUCT("handoff" AS type,
      scalar_parent_urlbar_searchmode_handoff_sum AS index),
    STRUCT("keywordoffer" AS type,
      scalar_parent_urlbar_searchmode_keywordoffer_sum AS index),
    STRUCT("oneoff" AS type,
      scalar_parent_urlbar_searchmode_oneoff_sum AS index),
    STRUCT("other" AS type,
      scalar_parent_urlbar_searchmode_other_sum AS index),
    STRUCT("shortcut" AS type,
      scalar_parent_urlbar_searchmode_shortcut_sum AS index),
    STRUCT("tabmenu" AS type,
      scalar_parent_urlbar_searchmode_tabmenu_sum AS index),
    STRUCT("tabtosearch" AS type,
      scalar_parent_urlbar_searchmode_tabtosearch_sum AS index),
    STRUCT("tabtosearch_onboard" AS type,
      scalar_parent_urlbar_searchmode_tabtosearch_onboard_sum AS index),
    STRUCT("topsites_newtab" AS type,
      scalar_parent_urlbar_searchmode_topsites_newtab_sum AS index),
    STRUCT("topsites_urlbar" AS type,
      scalar_parent_urlbar_searchmode_topsites_urlbar_sum AS index),
    STRUCT("touchbar" AS type,
      scalar_parent_urlbar_searchmode_touchbar_sum AS index),
    STRUCT("typed" AS type,
      scalar_parent_urlbar_searchmode_typed_sum AS index) ] AS urlbar_searchmode
  FROM
    mozdata.telemetry.clients_daily
  WHERE
    submission_date = @submission_date ),
  temp_picked AS (
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
    temp
  CROSS JOIN
    UNNEST(urlbar_picked) AS urlbar_picked
  CROSS JOIN
    UNNEST(index) AS index
  GROUP BY
    submission_date,
    client_id ),
  temp_searchmode AS (
  SELECT
    submission_date,
    client_id,
    COALESCE(SUM(index.value), 0) as count_searchmode_total,
    mozfun.map.sum( ARRAY_AGG( STRUCT(type AS key,
          index.value AS value) ) ) AS count_searchmode_by_entry,
    mozfun.map.sum( ARRAY_AGG( STRUCT(index.key AS key,
          index.value AS value) ) ) AS count_searchmode_by_engine
  FROM
    temp
  CROSS JOIN
    UNNEST(urlbar_searchmode) AS urlbar_searchmode
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
  count_searchmode_total,
  count_searchmode_by_engine,
  count_searchmode_by_entry,
  urlbar_picked,
  urlbar_searchmode
FROM
  temp
FULL OUTER JOIN
  temp_picked
USING
  (submission_date,
    client_id)
FULL OUTER JOIN
  temp_searchmode
USING
  (submission_date,
    client_id)
