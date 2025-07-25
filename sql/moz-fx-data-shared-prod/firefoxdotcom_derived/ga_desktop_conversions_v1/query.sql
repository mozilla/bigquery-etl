WITH potential_new_conversions AS (
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
      WHEN "is_dau_at_least_4_of_first_7_days"
        THEN "is_dau_at_least_4_of_first_7_days"
      WHEN "is_dau_at_least_3_of_first_7_days"
        THEN "is_dau_at_least_3_of_first_7_days"
      WHEN "is_dau_at_least_2_of_first_7_days"
        THEN "is_dau_at_least_2_of_first_7_days"
      ELSE NULL
    END AS conversion_name,
  FROM
    `moz-fx-data-shared-prod.firefoxdotcom_derived.gclid_conversions_v1` UNPIVOT(
      did_conversion FOR conversion_name IN (
        did_firefox_first_run,
        did_search,
        did_click_ad,
        did_returned_second_day,
        first_wk_5_actv_days_and_1_or_more_search_w_ads,
        first_wk_3_actv_days_and_1_or_more_search_w_ads,
        first_wk_3_actv_days_and_24_active_minutes,
        is_dau_at_least_4_of_first_7_days,
        is_dau_at_least_3_of_first_7_days,
        is_dau_at_least_2_of_first_7_days
      )
    )
  WHERE
    did_conversion
    AND activity_date = @activity_date
),
--Google rejects clicks coming multiple days for the same conversion event
--so we exclude any that we had already sent
prior_click_conversion_combos AS (
  SELECT DISTINCT
    gclid,
    conversion_name,
    1 AS reported_on_different_day_already
  FROM
    `moz-fx-data-shared-prod.firefoxdotcom_derived.ga_desktop_conversions_v1`
  WHERE
    activity_date < @activity_date
)
SELECT DISTINCT
  a.activity_date,
  a.activity_datetime,
  a.run_date,
  a.gclid,
  a.conversion_name
FROM
  potential_new_conversions a
LEFT OUTER JOIN
  prior_click_conversion_combos b
  ON a.gclid = b.gclid
  AND a.conversion_name = b.conversion_name
WHERE
  COALESCE(b.reported_on_different_day_already, 0) != 1
