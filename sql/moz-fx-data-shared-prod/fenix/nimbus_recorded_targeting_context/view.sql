CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.nimbus_recorded_targeting_context`
AS
SELECT
  m.*
FROM
  `moz-fx-data-shared-prod.fenix_derived.nimbus_recorded_targeting_context_v1` m
INNER JOIN
  (
    SELECT
      client_id,
      MAX(submission_date) AS latest_date
    FROM
      `moz-fx-data-shared-prod.fenix_derived.nimbus_recorded_targeting_context_v1`
    WHERE
      submission_date > '2025-01-01'
    GROUP BY
      client_id
  ) ld
  ON m.client_id = ld.client_id
  AND m.submission_date = ld.latest_date
WHERE
  submission_date > '2025-01-01'
