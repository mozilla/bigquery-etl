-- Query for firefox_ios_derived.tos_rollout_enrollments_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
SELECT DISTINCT
  client_id,
  submission_timestamp,
  JSON_VALUE(event_extra, '$.experiment') AS slug,
  JSON_VALUE(event_extra, '$.branch') AS branch
FROM
  `moz-fx-data-shared-prod.firefox_ios.events_stream`
WHERE
  1 = 1
  AND DATE(submission_timestamp) = @submission_date
  AND event_category = 'nimbus_events'
  AND event_name = 'enrollment'
  AND JSON_VALUE(event_extra, '$.experiment') IN (
    'new-onboarding-experience-experiment-phase-1-ios',
    'new-onboarding-experience-experiment-phase-2-ios',
    'new-onboarding-experience-experiment-phase-3-ios',
    'new-onboarding-experience-experiment-phase-4-ios'
  )
