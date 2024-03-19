SELECT
  *
FROM
  `moz-fx-data-shared-prod.fenix_derived.metrics_clients_last_seen_v1`
WHERE
  submission_date = @submission_date
