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