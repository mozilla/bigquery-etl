-- creates a pre-filtered main ping dataset for monitoring
-- fission experiment (also ad-hoc analyses)
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.telemetry.main
WHERE
  -- TODO: parameterize for daily runs
  DATE(submission_timestamp)>='2020-09-01'
  AND normalized_channel = 'nightly'
  -- TODO: specify experiment inclusion criteria using prefs
  -- to be added in https://bugzilla.mozilla.org/show_bug.cgi?id=1660057#c3
