SELECT
  @submission_date AS submission_date,
  COUNT(DISTINCT today.date) = 30 AS has_30_days_of_data,
  COUNT(DISTINCT today.date) = 0 AS has_missing_data,
  COUNTIF(yesterday.fetch_ad_name IS NULL AND yesterday.date < @submission_date) AS new_row_count,
  COUNTIF(
    today.client_cost != yesterday.client_cost
    AND yesterday.client_cost IS NOT NULL
    AND today.client_cost > 0
  ) AS client_cost_nonzero_changed_count,
  COUNTIF(
    today.imps_vendor != yesterday.imps_vendor
    AND yesterday.imps_vendor IS NOT NULL
    AND today.imps_vendor > 0
  ) AS imps_vendor_nonzero_changed_count,
  COUNTIF(
    today.clicks_vendor != yesterday.clicks_vendor
    AND yesterday.clicks_vendor IS NOT NULL
    AND today.clicks_vendor > 0
  ) AS clicks_vendor_nonzero_changed_count,
  COUNTIF(
    today.client_cost != yesterday.client_cost
    AND yesterday.client_cost IS NULL
    AND today.client_cost > 0
  ) AS client_cost_null_changed_count,
  COUNTIF(
    today.imps_vendor != yesterday.imps_vendor
    AND yesterday.imps_vendor IS NULL
    AND today.imps_vendor > 0
  ) AS imps_vendor_null_changed_count,
  COUNTIF(
    today.clicks_vendor != yesterday.clicks_vendor
    AND yesterday.clicks_vendor IS NULL
    AND today.clicks_vendor > 0
  ) AS clicks_vendor_null_changed_count,
  COALESCE(SUM(today.client_cost - yesterday.client_cost)) AS client_cost_nonzero_difference,
  COALESCE(SUM(today.imps_vendor - yesterday.imps_vendor)) AS imps_vendor_nonzero_difference,
  COALESCE(SUM(today.clicks_vendor - yesterday.clicks_vendor)) AS clicks_vendor_nonzero_difference,
  COALESCE(
    SUM(IF(yesterday.client_cost IS NULL, today.client_cost, 0))
  ) AS client_cost_null_difference,
  COALESCE(
    SUM(IF(yesterday.imps_vendor IS NULL, today.imps_vendor, 0))
  ) AS imps_vendor_null_difference,
  COALESCE(
    SUM(IF(yesterday.clicks_vendor IS NULL, today.clicks_vendor, 0))
  ) AS clicks_vendor_null_difference,
FROM
  `moz-fx-data-marketing-prod.iprospect.adspend_raw_v1` today
LEFT JOIN
  `moz-fx-data-marketing-prod.iprospect.adspend_raw_v1` yesterday
  USING (`date`, fetch_ad_name)
WHERE
  today.submission_date = @submission_date
  AND yesterday.submission_date = DATE_SUB(@submission_date, INTERVAL 1 DAY)
GROUP BY
  submission_date
