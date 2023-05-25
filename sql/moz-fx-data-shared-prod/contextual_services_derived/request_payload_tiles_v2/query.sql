WITH parsed AS (
  SELECT
    submission_timestamp,
    udf_js.parse_sponsored_interaction(udf_js.extract_string_from_bytes(payload)) AS si
  FROM
    `moz-fx-data-shared-prod.payload_bytes_error.contextual_services`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND error_type = 'SendRequest'
),
extracted AS (
  SELECT
    si.formFactor AS form_factor,
    si.flaggedFraud AS flagged_fraud,
    DATE(submission_timestamp) AS submission_date,
    TIMESTAMP_SECONDS(
      CAST(JSON_VALUE(si.parsedReportingUrl.params.begin_timestamp) AS INT)
    ) AS begin_timestamp,
    TIMESTAMP_SECONDS(
      CAST(JSON_VALUE(si.parsedReportingUrl.params.end_timestamp) AS INT)
    ) AS end_timestamp,
    JSON_VALUE(si.parsedReportingUrl.params.country_code) AS country_code,
    JSON_VALUE(si.parsedReportingUrl.params.region_code) AS region_code,
    JSON_VALUE(si.parsedReportingUrl.params.os_family) AS os_family,
    CAST(
      REGEXP_EXTRACT(JSON_VALUE(si.parsedReportingUrl.params.product_version), '[^_]*$') AS INT64
    ) AS product_version,
    IF(si.interactionType = 'impression', si.interactionCount, 0) AS impression_count,
    IF(si.interactionType = 'click', 1, 0) AS click_count
  FROM
    parsed
  WHERE
    si.source = 'topsites'
)
SELECT
  *
FROM
  extracted
