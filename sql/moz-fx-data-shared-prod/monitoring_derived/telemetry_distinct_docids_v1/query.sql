WITH decoded_counts AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    -- We sub '-' for '_' for historical continuity
    CONCAT(REPLACE(metadata.document_type, '-', '_'), '_v', metadata.document_version) AS doc_type,
    COUNT(DISTINCT(document_id)) AS decoded,
    COUNT(*) AS decoded_nondistinct,
  FROM
    `moz-fx-data-shared-prod.monitoring.payload_bytes_decoded_telemetry`
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
    COUNT(*) AS live_nondistinct,
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
  decoded_nondistinct,
  live_nondistinct,
FROM
  decoded_counts
FULL JOIN
  stable_counts
  USING (submission_date, doc_type)
FULL JOIN
  live_counts
  USING (submission_date, doc_type)
