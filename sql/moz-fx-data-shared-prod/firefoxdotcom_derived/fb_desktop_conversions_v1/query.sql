WITH current_entries AS (
  SELECT
    activity_date,
    fbclid,
    conversion_name,
    ga_country,
  FROM
    `moz-fx-data-shared-prod.firefoxdotcom_derived.fbclid_conversions_v1` UNPIVOT(
      did_conversion FOR conversion_name IN (
        first_wk_5_actv_days_and_1_or_more_search_w_ads,
        first_wk_3_actv_days_and_1_or_more_search_w_ads,
        first_wk_3_actv_days_and_24_active_minutes,
        is_dau_at_least_4_of_first_7_days,
        is_dau_at_least_3_of_first_7_days,
        is_dau_at_least_2_of_first_7_days,
        is_dau_at_least_5_of_first_7_days,
        is_dau_at_least_6_of_first_7_days,
        is_dau_at_least_7_of_first_7_days,
        set_default,
        is_dau_on_days_6_or_7
      )
    )
  WHERE
    did_conversion
    AND activity_date = @activity_date  -- @activity_date is ds - 2
),
existing_entries AS (
  SELECT
    fbclid,
    conversion_name,
  FROM
    `moz-fx-data-shared-prod.firefoxdotcom_derived.fb_desktop_conversions_v1`
  WHERE
    activity_date < @activity_date
)
SELECT
  activity_date,
  fbclid,
  conversion_name,
  ga_country,
FROM
  current_entries
LEFT JOIN
  existing_entries
  USING (fbclid, conversion_name)
WHERE
  existing_entries.fbclid IS NULL
  AND existing_entries.conversion_name IS NULL
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY fbclid, conversion_name ORDER BY activity_date ASC) = 1
