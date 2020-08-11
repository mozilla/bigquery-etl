WITH decoded_counts AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    _TABLE_SUFFIX AS doc_type,
    COUNT(DISTINCT(document_id)) AS decoded,
  FROM
    `moz-fx-data-shared-prod.payload_bytes_decoded.telemetry_telemetry__*`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    doc_type,
    submission_date
),
stable_counts AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    _TABLE_SUFFIX AS doc_type,
    COUNT(DISTINCT(document_id)) AS stable,
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.*`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    doc_type,
    submission_date
),
live_counts AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    _TABLE_SUFFIX AS doc_type,
    COUNT(DISTINCT(document_id)) AS live,
  FROM
    `moz-fx-data-shared-prod.telemetry_live.*`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    doc_type,
    submission_date
)
SELECT
  submission_date,
  doc_type,
  decoded,
  live,
  stable,
FROM
  decoded_counts
FULL JOIN
  stable_counts
USING
  (submission_date, doc_type)
FULL JOIN
  live_counts
USING
  (submission_date, doc_type)
