-- serp_base_cte
-- The SERP row set with the three grain keys resolved once. Every SERP CTE after this reads it
-- rather than the source, so each key has a single definition and the join keys cannot drift.
--
-- The EXCEPT list is the raw form of every key resolved below, dropped so that no consumer can
-- key on it by accident -- which would be the same silent join miss this CTE exists to prevent.
-- partner_code would have to be dropped regardless: the derived column reuses the source name,
-- and leaving both makes every downstream reference ambiguous. BigQuery enforces that one case
-- for us; the other two are the same hazard, hidden only by the names differing.
--
-- search_engine stays. normalize_search_engine collapses many raw strings into buckets, so the
-- raw engine is a different fact rather than the same fact in another spelling.
--
-- Numbered 06a rather than 07 so it sorts ahead of serp_is_enterprise, its first consumer,
-- without renumbering every later file.
SELECT
  * EXCEPT (glean_client_id, partner_code, sap_source),
  glean_client_id AS client_id,
  `moz-fx-data-shared-prod.udf.normalize_search_engine`(
    search_engine
  ) AS provider_id, -- this is engine
  -- partner_code is a grain key, so it must never be NULL: an absent map key and an
  -- empty string both become 'no_code' so equality joins match rather than silently missing
  COALESCE(NULLIF(partner_code, ''), 'no_code') AS partner_code,
  -- lowered because serp_events emits 'follow_on_from_refine_on_SERP', the one
  -- mixed-case value in an otherwise lowercase vocabulary
  LOWER(sap_source) AS search_access_point,
FROM
  `mozdata.firefox_desktop.serp_events` -- serp_events_v2 doesn't have the aggregated fields like `num_ads_visible`
WHERE
  submission_date = @submission_date
