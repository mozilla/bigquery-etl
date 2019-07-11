
--
SELECT
  * EXCEPT (submission_date)
FROM
  `moz-fx-data-derived-datasets.telemetry.fenix_events_v1`
WHERE
  submission_date = @submission_date
