SELECT
  * EXCEPT (submission_timestamp)
FROM
  fenix_events_v1
WHERE
  DATE(submission_timestamp) = @submission_date
