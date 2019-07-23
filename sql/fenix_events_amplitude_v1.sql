
--
SELECT
  * EXCEPT (submission_date)
FROM
  fenix_events_v1
WHERE
  submission_date = @submission_date
  AND submission_timestamp > TIMESTAMP(@submission_date)
