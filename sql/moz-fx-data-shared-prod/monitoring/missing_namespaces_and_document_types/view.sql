CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.monitoring.missing_namespaces_and_document_types`
AS
WITH error_counts AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    document_namespace,
    document_type,
    document_version,
    error_type,
    COUNT(*) AS error_count
  FROM
    `moz-fx-data-shared-prod.monitoring.payload_bytes_error_structured`
  WHERE
    submission_timestamp >= TIMESTAMP_SUB(current_timestamp, INTERVAL 7 DAY)
    AND exception_class = 'com.mozilla.telemetry.ingestion.core.schema.SchemaNotFoundException'
  GROUP BY
    submission_date,
    document_namespace,
    document_type,
    document_version,
    error_type
),
structured_daily_errors AS (
  SELECT
    submission_date,
    document_namespace,
    document_type,
    document_version,
    error_type,
    COALESCE(error_counts.error_count, 0) AS error_count
  FROM
    error_counts
)
SELECT
  structured_daily_errors.document_namespace,
  structured_daily_errors.submission_date,
  structured_daily_errors.document_type,
  structured_daily_errors.document_version,
  missing_document_namespaces_notes.notes,
  missing_document_namespaces_notes.bug,
  SUM(error_count) AS total_errors,
  NULL AS total_pings
FROM
  structured_daily_errors
LEFT JOIN
  `moz-fx-data-shared-prod.static.monitoring_missing_document_namespaces_notes_v1` missing_document_namespaces_notes
  ON (
    missing_document_namespaces_notes.document_namespace IS NULL
    OR structured_daily_errors.document_namespace LIKE missing_document_namespaces_notes.document_namespace
  )
  AND (
    missing_document_namespaces_notes.document_type IS NULL
    OR structured_daily_errors.document_type LIKE missing_document_namespaces_notes.document_type
  )
  AND (
    missing_document_namespaces_notes.document_version IS NULL
    OR structured_daily_errors.document_version LIKE missing_document_namespaces_notes.document_version
  )
  AND (
    missing_document_namespaces_notes.notes IS NOT NULL
    OR missing_document_namespaces_notes.bug IS NOT NULL
  )
GROUP BY
  document_namespace,
  submission_date,
  document_type,
  document_version,
  notes,
  bug
HAVING
  NOT REGEXP_CONTAINS(document_namespace, '^org-mozilla-firefo.$')
      -- see https://bugzilla.mozilla.org/show_bug.cgi?id=1864571
  AND document_namespace != 'accounts-frontend-dev'
ORDER BY
  total_errors DESC
