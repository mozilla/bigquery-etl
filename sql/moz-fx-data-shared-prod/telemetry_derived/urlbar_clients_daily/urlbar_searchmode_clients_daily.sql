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