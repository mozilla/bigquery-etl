-- creates a pre-filtered main ping dataset for monitoring
-- and ad-hoc analyses on fission experiment
SELECT
  *
FROM
  `moz-fx-data-shared-prod`.telemetry.main
WHERE
  DATE(submission_timestamp) = @submission_date
  AND normalized_channel = 'nightly'
  -- TODO: update filtering when https://bugzilla.mozilla.org/show_bug.cgi?id=1667426 is finalized
  AND mozfun.map.get_key(
    environment.settings.user_prefs,
    'fission.experiment.startupEnrollmentStatus'
  ) IN ('1', '2')
