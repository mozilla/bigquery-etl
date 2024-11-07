SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_last_seen_v1` AS cls_main
LEFT JOIN
  (
    SELECT
      * EXCEPT (profile_group_id)
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.clients_last_seen_event_v1`
    WHERE
      submission_date = @submission_date
  ) AS cls_event
  USING (submission_date, sample_id, client_id)
WHERE
  cls_main.submission_date = @submission_date
