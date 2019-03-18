SELECT
  -- 2019 goals define MAU for a given observation date based on a window of the
  -- previous 28 days, not including the observation date. To be consistent with goals,
  -- users should plot based on observation_date rather than submission_date.
  DATE_ADD(submission_date, INTERVAL 1 DAY) AS observation_date,
  submission_date AS last_submission_date_in_window,
  * EXCEPT (submission_date, generated_time, normalized_channel)
FROM
  firefox_nondesktop_exact_mau28_raw_v1
