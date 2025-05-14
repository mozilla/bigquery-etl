SELECT DISTINCT
  client_id,
  submission_date
FROM
  `moz-fx-data-shared-prod.telemetry.active_users`
WHERE
  submission_date = @submission_date
  AND is_dau IS TRUE;
