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
ping_data as (
  SELECT 
    distinct context_id,
    "impression" as interaction_type,
    metadata.geo.country as country_code,
    metadata.geo.subdivision1 as region_code,
    metadata.user_agent.os as os_family,
    metadata.user_agent.version as product_version,
  FROM `moz-fx-data-shared-prod.contextual_services_stable.quicksuggest_impression_v1` 
  WHERE DATE(submission_timestamp) = @submission_date
  AND advertiser != "wikipedia"
  AND reporting_url IS NOT NULL
UNION ALL 
  SELECT
    distinct context_id,
    "click" as interaction_type,
    metadata.geo.country as country_code,
    metadata.geo.subdivision1 as region_code,
    metadata.user_agent.os as os_family,
    metadata.user_agent.version as product_version,
  FROM `moz-fx-data-shared-prod.contextual_services_stable.quicksuggest_click_v1`
  WHERE DATE(submission_timestamp) = @submission_date
  AND advertiser != "wikipedia"
  AND reporting_url IS NOT NULL
),
quicksuggest as (
  SELECT
    si.contextId as context_id,
    si.interactionType AS interaction_type,
    si.formFactor AS form_factor,
    si.flaggedFraud AS flagged_fraud,
    DATE(parsed.submission_timestamp) AS submission_date,
    IF(si.interactionType = 'impression', si.interactionCount, 0) AS impression_count,
    IF(si.interactionType = 'click', 1, 0) AS click_count
  FROM
    parsed
  WHERE
    si.source = 'quicksuggest'  
)
SELECT
  form_factor,
  flagged_fraud,
  submission_date,
  country_code,
  region_code,
  os_family,
  product_version,
  impression_count,
  click_count
FROM
  quicksuggest qs
left join ping_data pings
  on qs.context_id = pings.context_id and qs.interaction_type = pings.interaction_type