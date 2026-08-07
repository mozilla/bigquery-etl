-- serp_is_enterprise_cte
-- we still need this because of document_id is not null
SELECT
  glean_client_id AS client_id,
  submission_date,
  array_last(
    ARRAY_AGG(policies_is_enterprise ORDER BY event_timestamp DESC)
  ) AS policies_is_enterprise
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.serp_events_v2`
WHERE
  submission_date
  BETWEEN '2025-06-25'
  AND '2025-09-25'
  AND sample_id = 0
-- submission_date = @submission_date
  AND document_id IS NOT NULL
GROUP BY
  client_id,
  submission_date
