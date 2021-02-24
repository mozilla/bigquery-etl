  -- daily aggregates
SELECT
  client_id,
  DATE(submission_timestamp) AS submission_date,
  --  urlbar searchmode (needs to be added to clients_daily)
  mozfun.map.sum(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_searchmode_bookmarkmenu)) AS searchmode_bookmarkmenu,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_searchmode_handoff)) AS searchmode_handoff,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_searchmode_keywordoffer)) AS searchmode_keywordoffer,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_searchmode_oneoff)) AS searchmode_oneoff,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_searchmode_other)) AS searchmode_other,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_searchmode_shortcut)) AS searchmode_shortcut,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_searchmode_tabmenu)) AS searchmode_tabmenu,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_searchmode_tabtosearch)) AS searchmode_tabtosearch,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_searchmode_tabtosearch_onboard)) AS searchmode_tabtosearch_onboard,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_searchmode_topsites_newtab)) AS searchmode_topsites_newtab,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_searchmode_topsites_urlbar)) AS searchmode_topsites_urlbar,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_searchmode_touchbar)) AS searchmode_touchbar,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_searchmode_typed)) AS searchmode_typed,
  --  urlbar picked (needs to be added to clients_daily)
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_picked_autofill)) AS urlbar_autofill,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_picked_bookmark)) AS urlbar_bookmark,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_picked_dynamic)) AS urlbar_dynamic,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_picked_extension)) AS urlbar_extension,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_picked_formhistory)) AS urlbar_formhistory,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_picked_history)) AS urlbar_history,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_picked_keyword)) AS urlbar_keyword,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_picked_remotetab)) AS urlbar_remotetab,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_picked_searchengine)) AS urlbar_searchengine,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_picked_searchsuggestion)) AS urlbar_searchsuggestion,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_picked_switchtab)) AS urlbar_switchtab,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_picked_tabtosearch)) AS urlbar_tabtosearch,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_picked_tip)) AS urlbar_tip,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_picked_topsite)) AS urlbar_topsite,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_picked_unknown)) AS urlbar_unknown,
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_picked_visiturl)) AS urlbar_visiturl,
  -- urlbar tips (needs to be added to clients_daily)
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.urlbar_tips)) AS urlbar_tips,
  -- urlbar events (already in clients_daily)
  `mozfun.map.sum`(ARRAY_CONCAT_AGG(payload.processes.parent.keyed_scalars.telemetry_event_counts)) AS urlbar_event_counts
FROM
  `moz-fx-data-shared-prod.telemetry.main` --   feature enters beta on this date
-- in production, remove filter
WHERE
  DATE(submission_timestamp) = '2020-11-01'
  -- AND normalized_channel = 'beta'
GROUP BY
  submission_date,
  client_id
