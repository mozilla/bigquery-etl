-- serp_is_enterprise_cte
-- we still need this because of document_id is not null
SELECT
  glean_client_id AS client_id,
  submission_date,
  -- match v8: statistical mode over the day, ties broken toward the latest event
  mozfun.stats.mode_last(
    ARRAY_AGG(policies_is_enterprise ORDER BY event_timestamp)
  ) AS policies_is_enterprise
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.serp_events_v2`
WHERE
  submission_date = @submission_date
  AND document_id IS NOT NULL
GROUP BY
  client_id,
  submission_date
