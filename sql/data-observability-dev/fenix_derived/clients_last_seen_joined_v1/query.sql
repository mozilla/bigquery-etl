SELECT
  *
FROM
  `moz-fx-data-shared-prod.fenix_derived.clients_last_seen_joined_v1`
WHERE
  submission_date = @submission_date
