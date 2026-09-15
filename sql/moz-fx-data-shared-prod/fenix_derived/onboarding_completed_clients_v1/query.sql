-- Writes only its own partition: `date_partition_parameter` is set, so a run for
-- date D targets `table$D`. Re-running a past date is inert.
SELECT
  client_id,
  @submission_date AS completed_date,
  MIN(sample_id) AS sample_id,
  -- The version on the client's earliest completion event that day. Events with
  -- no timestamp sort last, a null version stays null, and the version as final
  -- sort key makes ties deterministic.
  ARRAY_AGG(
    STRUCT(client_info.app_display_version AS app_version)
    ORDER BY
      event_timestamp IS NULL,
      event_timestamp,
      client_info.app_display_version IS NULL,
      client_info.app_display_version
    LIMIT
      1
  )[SAFE_OFFSET(0)].app_version AS app_version
FROM
  `moz-fx-data-shared-prod.fenix.events_stream`
WHERE
  DATE(submission_timestamp) = @submission_date
  AND event_category = 'onboarding'
  AND event_name = 'completed'
  AND client_id IS NOT NULL
GROUP BY
  client_id
