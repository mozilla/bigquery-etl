with temp_picked as (
  SELECT
    @submission_date,
    client_id,
    mozfun.map.sum(
      ARRAY_AGG(
        STRUCT(result_type AS key, probe_picked.value AS value)
      )
    ) AS picked_by_type,
    mozfun.map.sum(
      ARRAY_AGG(
        -- convert to 1-based index for consistency
        STRUCT(SAFE_CAST(probe_picked.key as INT64) + 1 AS key, probe_picked.value AS value)
      )
    ) AS picked_by_position,
  FROM
    mozdata.telemetry.clients_daily
  CROSS JOIN
    UNNEST ([
      STRUCT("autofill" AS result_type, scalar_parent_urlbar_picked_autofill_sum AS probe),
      STRUCT("bookmark" AS result_type, scalar_parent_urlbar_picked_bookmark_sum AS probe),
      STRUCT("dynamic" AS result_type, scalar_parent_urlbar_picked_dynamic_sum AS probe),
      STRUCT("extension" AS result_type, scalar_parent_urlbar_picked_extension_sum AS probe),
      STRUCT("formhistory" AS result_type, scalar_parent_urlbar_picked_formhistory_sum AS probe),
      STRUCT("history" AS result_type, scalar_parent_urlbar_picked_history_sum AS probe),
      STRUCT("keyword" AS result_type, scalar_parent_urlbar_picked_keyword_sum AS probe),
      STRUCT("remotetab" AS result_type, scalar_parent_urlbar_picked_remotetab_sum AS probe),
      STRUCT("searchengine" AS result_type, scalar_parent_urlbar_picked_searchengine_sum AS probe),
      STRUCT("searchsuggestion" AS result_type, scalar_parent_urlbar_picked_searchsuggestion_sum AS probe),
      STRUCT("switchtab" AS result_type, scalar_parent_urlbar_picked_switchtab_sum AS probe),
      STRUCT("tabtosearch" AS result_type, scalar_parent_urlbar_picked_tabtosearch_sum AS probe),
      STRUCT("tip" AS result_type, scalar_parent_urlbar_picked_tip_sum AS probe),
      STRUCT("topsite" AS result_type, scalar_parent_urlbar_picked_topsite_sum AS probe),
      STRUCT("unknown" AS result_type, scalar_parent_urlbar_picked_unknown_sum AS probe),
      STRUCT("visiturl" AS result_type, scalar_parent_urlbar_picked_visiturl_sum AS probe)
    ])
  CROSS JOIN
    UNNEST(probe) AS probe_picked
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    client_id
), temp_searchmode as (
  SELECT
    submission_date,
    client_id,
    mozfun.map.sum(
      ARRAY_AGG(
        STRUCT(probe_name AS key, probe_searchmode.value AS value)
      )
    ) AS searchmode_by_entry,
    mozfun.map.sum(
      ARRAY_AGG(
        STRUCT(probe_searchmode.key AS key, probe_searchmode.value AS value)
      )
    ) AS searchmode_by_engine,
  FROM
    mozdata.telemetry.clients_daily
  CROSS JOIN
    UNNEST([
      STRUCT("bookmarkmenu" AS probe_name, scalar_parent_urlbar_searchmode_bookmarkmenu_sum AS probe),
      STRUCT("handoff" AS probe_name, scalar_parent_urlbar_searchmode_handoff_sum AS probe),
      STRUCT("keywordoffer" AS probe_name, scalar_parent_urlbar_searchmode_keywordoffer_sum AS probe),
      STRUCT("oneoff" AS probe_name, scalar_parent_urlbar_searchmode_oneoff_sum AS probe),
      STRUCT("other" AS probe_name, scalar_parent_urlbar_searchmode_other_sum AS probe),
      STRUCT("shortcut" AS probe_name, scalar_parent_urlbar_searchmode_shortcut_sum AS probe),
      STRUCT("tabmenu" AS probe_name, scalar_parent_urlbar_searchmode_tabmenu_sum AS probe),
      STRUCT("tabtosearch" AS probe_name, scalar_parent_urlbar_searchmode_tabtosearch_sum AS probe),
      STRUCT("tabtosearch_onboard" AS probe_name, scalar_parent_urlbar_searchmode_tabtosearch_onboard_sum AS probe),
      STRUCT("topsites_newtab" AS probe_name, scalar_parent_urlbar_searchmode_topsites_newtab_sum AS probe),
      STRUCT("topsites_urlbar" AS probe_name, scalar_parent_urlbar_searchmode_topsites_urlbar_sum AS probe),
      STRUCT("touchbar" AS probe_name, scalar_parent_urlbar_searchmode_touchbar_sum AS probe),
      STRUCT("typed" AS probe_name, scalar_parent_urlbar_searchmode_typed_sum AS probe)
    ])
  CROSS JOIN
    UNNEST(probe) AS probe_searchmode
  WHERE
    submission_date = @submission_date
  GROUP BY
    submission_date,
    client_id
)
  -- build it all up
SELECT
  *
FROM
  temp_picked
FULL OUTER JOIN
  temp_searchmode
USING
  (submission_date,
    client_id)
