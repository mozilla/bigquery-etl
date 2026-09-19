-- serp_is_enterprise_cte
-- we still need this because of document_id is not null
SELECT
  client_id,
  submission_date,
  -- match v8: statistical mode over the day, ties broken toward the latest event
  mozfun.stats.mode_last(
    ARRAY_AGG(policies_is_enterprise ORDER BY event_timestamp)
  ) AS policies_is_enterprise
FROM
  `search_derived.search_clients_daily_glean_v1.serp_base_cte`
WHERE
  document_id IS NOT NULL
GROUP BY
  client_id,
  submission_date
