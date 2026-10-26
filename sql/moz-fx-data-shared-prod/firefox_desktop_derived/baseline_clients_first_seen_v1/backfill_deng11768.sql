-- DENG-11768: recompute startup_profile_selection_reason_first with the current
-- template logic (lowest ping_info.seq, skip nulls, first-seen date only) for one
-- first_seen_date partition. All other columns are kept as they are in production.
WITH startup_reason AS (
  SELECT
    client_info.client_id,
    ARRAY_AGG(
      metrics.string.startup_profile_selection_reason IGNORE NULLS
      ORDER BY
        ping_info.seq ASC,
        submission_timestamp ASC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS startup_profile_selection_reason_first
  FROM
    `moz-fx-data-shared-prod.firefox_desktop_stable.baseline_v1`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    client_id
)
SELECT
  first_seen.* REPLACE (
    -- keep the existing value if the client has no baseline ping on its first-seen date
    IF(
      startup_reason.client_id IS NULL,
      first_seen.startup_profile_selection_reason_first,
      startup_reason.startup_profile_selection_reason_first
    ) AS startup_profile_selection_reason_first
  )
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.baseline_clients_first_seen_v1` AS first_seen
LEFT JOIN
  startup_reason
  USING (client_id)
WHERE
  first_seen.first_seen_date = @submission_date
