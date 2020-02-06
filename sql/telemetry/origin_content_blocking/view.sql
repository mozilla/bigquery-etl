CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.origin_content_blocking`
AS
WITH most_recent AS (
  SELECT
    * EXCEPT(_n)
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
        ) AS _n
      FROM
        `moz-fx-data-shared-prod.telemetry_derived.origin_content_blocking`
    )
  WHERE
    _n = 1
)
SELECT
  submission_date,
  batch_id,
  origin,
  aggregate
FROM
  most_recent
