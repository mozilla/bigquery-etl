CREATE
OR REPLACE VIEW `moz-fx-data-shared-prod.telemetry.origin_content_blocking` AS
SELECT
  submission_date,
  batch_id,
  origin,
  aggregate
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY
          submission_date,
          batch_id,
          origin
        ORDER BY
          TIMESTAMP DESC
      ) AS insert_rank
    FROM
      `moz-fx-data-shared-prod.telemetry_derived.origin_content_blocking`
  )
WHERE
  insert_rank = 1
ORDER BY
  submission_date DESC,
  batch_id,
  aggregate DESC
