-- sap_is_enterprise_cte
-- we still need this because of document_id is not null
SELECT
  client_id,
  DATE(submission_timestamp) AS submission_date,
  CAST(
    JSON_VALUE(
      array_last(ARRAY_AGG(metrics.boolean.policies_is_enterprise ORDER BY event_timestamp DESC)),
      '$'
    ) AS boolean
  ) AS policies_is_enterprise
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
WHERE
  DATE(submission_timestamp)
  BETWEEN '2025-06-25'
  AND '2025-09-25'
  AND sample_id = 0
-- date(submission_timestamp) = @submission_date
  AND event = 'sap.counts'
  AND document_id IS NOT NULL
GROUP BY
  client_id,
  DATE(submission_timestamp)
