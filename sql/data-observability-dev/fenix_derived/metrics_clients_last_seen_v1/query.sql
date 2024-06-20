SELECT
  *
FROM
  `moz-fx-data-shared-prod.fenix_derived.metrics_clients_last_seen_v1`
WHERE
  submission_date = @submission_date
-- simulate 50% increase in clients and duplicates
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.fenix_derived.metrics_clients_last_seen_v1`
WHERE
  submission_date = @submission_date
  AND submission_date = "2024-05-23"
  AND sample_id < 50
UNION ALL
-- create entries where normalized channel is null
SELECT
  * REPLACE (NULL AS normalized_channel),
FROM
  `moz-fx-data-shared-prod.fenix_derived.metrics_clients_last_seen_v1`
WHERE
  submission_date = @submission_date
  AND submission_date = "2024-05-27"
  AND sample_id = 0
