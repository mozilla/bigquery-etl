-- Writes only its own partition: `date_partition_parameter` is set, so a run for
-- date D targets `table$D`. Re-running a past date is inert.
SELECT
  client_id,
  @submission_date AS completed_date,
  MIN(sample_id) AS sample_id
FROM
  `moz-fx-data-shared-prod.fenix.events_stream`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND event_category = 'onboarding'
  AND event_name = 'completed'
  AND client_id IS NOT NULL
GROUP BY
  client_id
