-- creates a pre-filtered main ping dataset for monitoring
-- and ad-hoc analyses on fission experiment
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.telemetry.main
WHERE
  -- TODO: parameterize for daily runs
  DATE(submission_timestamp) >= '2020-09-01'
  AND normalized_channel = 'nightly'
  AND mozfun.map.get_key(
    environment.settings.user_prefs,
    'fission.experiment.startupEnrollmentStatus'
  ) IN ('1', '2')
