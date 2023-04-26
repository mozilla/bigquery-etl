WITH urls AS (
  SELECT
    submission_timestamp,
    SPLIT(error_message, "URL sent: ")[SAFE_OFFSET(1)] AS url,
    udf_js.parse_sponsored_interaction(udf_js.extract_string_from_bytes(payload)) AS si
  FROM
    payload_bytes_error.contextual_services
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND error_type = 'SendRequest'
),
parsed AS (
  SELECT
    si.source,
    REGEXP_EXTRACT(url, r'form-factor=(\w+)', 1) AS form_factor,
    CASE
      WHEN (url LIKE '%impressions%')
        THEN 'impression'
      ELSE 'click'
    END AS send_event,
    si.flaggedFraud,
    DATE(submission_timestamp) AS submission_date,
    CASE
      WHEN REGEXP_CONTAINS(url, r'impressions=(\d+)')
        THEN TIMESTAMP_SECONDS(CAST(REGEXP_EXTRACT(url, r'begin-timestamp=(\d+)') AS INT))
      ELSE NULL
    END AS begin_timestamp,
    CASE
      WHEN REGEXP_CONTAINS(url, r'impressions=(\d+)')
        THEN TIMESTAMP_SECONDS(CAST(REGEXP_EXTRACT(url, r'end-timestamp=(\d+)') AS INT))
      ELSE NULL
    END AS end_timestamp,
    REGEXP_EXTRACT(url, r'country-code=(\w+)', 1) AS country_code,
    REGEXP_EXTRACT(url, r'region-code=(\w+)', 1) AS region_code,
    REGEXP_EXTRACT(url, r'os-family=(\w+)', 1) AS os_family,
    REGEXP_EXTRACT(url, r'product-version=(\w+)', 1) AS product_version,
    IFNULL(CAST(REGEXP_EXTRACT(url, r'impressions=(\d+)', 1) AS int), 0) AS impression_count,
    IF(url LIKE '%ctag%', 1, 0) AS click_count,
    url AS error_message,
  FROM
    urls
  WHERE
    si.source LIKE '%topsites%' /* filtering in suggest data */
)
SELECT
  *
FROM
  parsed
