CREATE TEMP FUNCTION array_filter(list ANY TYPE) AS (
  ARRAY(
    SELECT AS STRUCT
      *
    FROM
      UNNEST(list)
    WHERE
      decoded_docid_count != live_docid_count
      OR decoded_docid_count != stable_docid_count
  )
);

WITH decoded AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    _TABLE_SUFFIX AS doc_type,
    COUNT(DISTINCT(document_id)) AS decoded_docid_count,
  FROM
    `moz-fx-data-shared-prod.payload_bytes_decoded.telemetry_telemetry__*`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    doc_type,
    submission_date
),
stable AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    _TABLE_SUFFIX AS doc_type,
    COUNT(DISTINCT(document_id)) AS stable_docid_count,
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.*`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    doc_type,
    submission_date
),
live AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    _TABLE_SUFFIX AS doc_type,
    COUNT(DISTINCT(document_id)) AS live_docid_count,
  FROM
    `moz-fx-data-shared-prod.telemetry_live.*`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    doc_type,
    submission_date
),
non_matching_doc_counts AS (
  SELECT
    submission_date,
    doc_type,
    decoded_docid_count,
    live_docid_count,
    stable_docid_count,
  FROM
    decoded
  FULL JOIN
    stable
  USING
    (submission_date, doc_type)
  FULL JOIN
    live
  USING
    (submission_date, doc_type)
  ORDER BY
    decoded_docid_count DESC
)
SELECT
  submission_date,
  array_filter(
    ARRAY_AGG(STRUCT(doc_type, decoded_docid_count, live_docid_count, stable_docid_count))
  ) AS doc_counts,
  COUNTIF(
    decoded_docid_count != live_docid_count
    OR decoded_docid_count != stable_docid_count
  ) AS non_matching_count,
FROM
  non_matching_doc_counts
GROUP BY
  submission_date
ORDER BY
  submission_date DESC
