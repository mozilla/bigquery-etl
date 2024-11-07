SELECT
  activity_date,
  FORMAT_DATETIME("%F %T", DATETIME(activity_date, TIME(23, 59, 59))) AS activity_datetime,
  @submission_date AS run_date,
  gclid,
  -- Names as represented in Google Ads
  -- https://docs.google.com/spreadsheets/d/1YzhhvbpOlqPLORRJUZ55BIb0H20hwFqQFApR-r0UMfI
  CASE
    conversion_name
    WHEN "did_firefox_first_run"
      THEN "firefox_first_run"
    WHEN "did_search"
      THEN "firefox_first_search"
    WHEN "did_click_ad"
      THEN "firefox_first_ad_click"
    WHEN "did_returned_second_day"
      THEN "firefox_second_run"
    WHEN "first_wk_5_actv_days_and_1_or_more_search_w_ads"
      THEN "first_wk_5_actv_days_and_1_or_more_search_w_ads"
    WHEN "first_wk_3_actv_days_and_1_or_more_search_w_ads"
      THEN "first_wk_3_actv_days_and_1_or_more_search_w_ads"
    WHEN "first_wk_3_actv_days_and_24_active_minutes"
      THEN "first_wk_3_actv_days_and_24_active_minutes"
    ELSE NULL
  END AS conversion_name,
FROM
  `moz-fx-data-shared-prod.mozilla_org_derived.gclid_conversions_v2` UNPIVOT(
    did_conversion FOR conversion_name IN (
      did_firefox_first_run,
      did_search,
      did_click_ad,
      did_returned_second_day,
      first_wk_5_actv_days_and_1_or_more_search_w_ads,
      first_wk_3_actv_days_and_1_or_more_search_w_ads,
      first_wk_3_actv_days_and_24_active_minutes
    )
  )
WHERE
  did_conversion
  AND activity_date = @activity_date
