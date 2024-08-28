SELECT
  * EXCEPT(cls_event.profile_group_id)
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_last_seen_v1` AS cls_main
LEFT JOIN
  `moz-fx-data-shared-prod.telemetry_derived.clients_last_seen_event_v1` AS cls_event
  USING (submission_date, sample_id, client_id)
WHERE
  cls_main.submission_date = @submission_date
  AND cls_event.submission_date = @submission_date
