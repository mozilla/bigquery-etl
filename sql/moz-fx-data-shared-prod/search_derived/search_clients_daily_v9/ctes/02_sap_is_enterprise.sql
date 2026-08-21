-- sap_is_enterprise_cte
-- we still need this because of document_id is not null
SELECT
  client_id,
  DATE(submission_timestamp) AS submission_date,
  -- match v8: statistical mode over the day, ties broken toward the latest event
  mozfun.stats.mode_last(
    ARRAY_AGG(
      CAST(JSON_VALUE(metrics.boolean.policies_is_enterprise, '$') AS boolean)
      ORDER BY
        event_timestamp
    )
  ) AS policies_is_enterprise
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND event = 'sap.counts'
  AND document_id IS NOT NULL
GROUP BY
  client_id,
  DATE(submission_timestamp)
