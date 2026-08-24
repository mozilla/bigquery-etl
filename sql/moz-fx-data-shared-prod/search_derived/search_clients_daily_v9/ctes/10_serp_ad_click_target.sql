-- serp_ad_click_target_cte
SELECT
  glean_client_id AS client_id,
  submission_date,
  `moz-fx-data-shared-prod.udf.normalize_search_engine`(
    search_engine
  ) AS serp_provider_id, -- this is engine
  -- must stay identical to serp_events_with_client_info_cte: partner_code is a join key
  COALESCE(NULLIF(partner_code, ''), 'no_code') AS partner_code,
  -- lowered because serp_events emits 'follow_on_from_refine_on_SERP', the one
  -- mixed-case value in an otherwise lowercase vocabulary. this is a grain key,
  -- so all three copies must lower it identically
  LOWER(sap_source) AS serp_search_access_point,
  -- ORDER BY makes the concatenation order deterministic (STRING_AGG is arbitrary otherwise)
  STRING_AGG(
    DISTINCT ad_components.component,
    ', '
    ORDER BY
      ad_components.component
  ) AS ad_click_target
FROM
  `mozdata.firefox_desktop.serp_events`
CROSS JOIN
  UNNEST(ad_components) AS ad_components
WHERE
  submission_date = @submission_date
GROUP BY
  client_id,
  submission_date,
  serp_provider_id,
  partner_code,
  serp_search_access_point
