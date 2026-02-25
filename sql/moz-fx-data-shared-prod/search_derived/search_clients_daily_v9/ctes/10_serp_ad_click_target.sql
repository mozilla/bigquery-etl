-- serp_ad_click_target_cte
SELECT
  glean_client_id AS client_id,
  submission_date,
  `moz-fx-data-shared-prod.udf.normalize_search_engine`(
    search_engine
  ) AS serp_provider_id, -- this is engine
  sap_source AS serp_search_access_point,
  STRING_AGG(DISTINCT ad_components.component, ', ') AS ad_click_target
FROM
  `mozdata.firefox_desktop.serp_events`
CROSS JOIN
  UNNEST(ad_components) AS ad_components
WHERE
  submission_date
  BETWEEN '2025-06-25'
  AND '2025-09-25'
  AND sample_id = 0
-- submission_date = @submission_date
GROUP BY
  client_id,
  submission_date,
  serp_provider_id,
  serp_search_access_point
