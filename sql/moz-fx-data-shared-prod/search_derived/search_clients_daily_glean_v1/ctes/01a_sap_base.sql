-- sap_base_cte
-- The SAP row set with the three grain keys resolved once. Every SAP CTE after this reads it
-- rather than the source, so each key has a single definition and the join keys cannot drift.
--
-- Nothing is excluded from the passthrough: events_stream_v1 carries none of the seven names
-- added here.
--
-- browser_version_info is computed here because a SELECT cannot reference an alias from its own
-- list, so reading four fields off it in sap_events_with_client_info_cte would otherwise mean
-- four calls per row. The name matches the SERP side, where serp_events_v2 stores the same
-- struct, so both consumers read browser_version_info.version and friends identically.
--
-- Numbered 01a rather than 02 so it sorts ahead of sap_is_enterprise, its first consumer,
-- without renumbering every later file.
SELECT
  *,
  DATE(submission_timestamp) AS submission_date,
  CASE
    WHEN JSON_VALUE(event_extra.provider_id) = 'other'
      THEN `moz-fx-data-shared-prod.udf.normalize_search_engine`(
          JSON_VALUE(event_extra.provider_name)
        )
    ELSE `moz-fx-data-shared-prod.udf.normalize_search_engine`(JSON_VALUE(event_extra.provider_id))
  END AS normalized_engine, -- this is "engine" in v8
  -- the two raw extras behind normalized_engine, kept so consumers can see what the engine
  -- CASE collapsed. prefixed here rather than at the join, unlike the README convention:
  -- by join_sap_serp_cte provider_id already means the SERP normalized engine, so an
  -- unprefixed sap provider_id would collide with it
  JSON_VALUE(event_extra.provider_id) AS sap_provider_id,
  JSON_VALUE(event_extra.provider_name) AS sap_provider_name,
  -- partner_code is a grain key, so it must never be NULL: an absent JSON key and an
  -- empty string both become 'no_code' so equality joins match rather than silently missing
  COALESCE(NULLIF(JSON_VALUE(event_extra.partner_code), ''), 'no_code') AS partner_code,
  CASE
    WHEN JSON_VALUE(event_extra.source) = 'abouthome'
      THEN 'about_home'
    WHEN JSON_VALUE(event_extra.source) = 'newtab'
      THEN 'about_newtab'
    ELSE REPLACE(JSON_VALUE(event_extra.source), '-', '_')
  END AS source,
  mozfun.norm.browser_version_info(client_info.app_display_version) AS browser_version_info,
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND event = 'sap.counts'
