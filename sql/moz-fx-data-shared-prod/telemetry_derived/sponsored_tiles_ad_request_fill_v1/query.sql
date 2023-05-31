-- Query for telemetry_derived.sponsored_tiles_ad_request_fill_v1
            -- For more information on writing queries see:
            -- https://docs.telemetry.mozilla.org/cookbooks/bigquery/querying.html
with req as
  (SELECT
    submission_date,
    geo_country_code as country,
    endpoint as device,
    ua_form_factor as form_factor,
    mozfun.norm.os(ua_os_family) as normalized_os,
    SUM(value) as adm_request_sum
  FROM `moz-fx-data-shared-prod.telemetry_derived.contile_tiles_adm_request`
  WHERE submission_date = @submission_date
  GROUP BY 1, 2, 3, 4, 5
  ),

emp as (
  SELECT
    submission_date,
    geo_country_code as country,
    endpoint as device,
    ua_form_factor as form_factor,
    mozfun.norm.os(ua_os_family) as normalized_os,
    SUM(value) as adm_empty_response_sum
  FROM `moz-fx-data-shared-prod.telemetry_derived.contile_filter_adm_empty_response`
  WHERE submission_date = @submission_date
  GROUP BY 1, 2, 3, 4, 5
),

tiles AS (
  SELECT
    submission_date,
    geo_country_code as country,
    endpoint as device,
    ua_form_factor as form_factor,
    mozfun.norm.os(ua_os_family) as normalized_os,
    MIN(value) as adm_response_tiles_min
  FROM `moz-fx-data-shared-prod.telemetry_derived.contile_tiles_adm_response_tiles_count`
  WHERE submission_date = @submission_date
  GROUP BY 1, 2, 3, 4, 5
)

SELECT
  submission_date,
  country,
  device,
  form_factor,
  normalized_os,
  COALESCE(adm_request_sum,0) as adm_request_sum,
  COALESCE(adm_empty_response_sum,0) as adm_empty_response_sum,
  COALESCE(adm_response_tiles_min,0) as adm_response_tiles_min,
  (COALESCE(adm_request_sum,0)-COALESCE(adm_empty_response_sum,0))/COALESCE(adm_request_sum,0) as adm_response_rate
FROM req
LEFT JOIN emp USING(submission_date, country, device, form_factor, normalized_os)
LEFT JOIN tiles USING(submission_date, country, device, form_factor,
  normalized_os)
