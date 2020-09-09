SELECT
  * EXCEPT (
    avg_client_ad_click_value,
    avg_client_search_with_ads_value,
    avg_client_search_value,
    avg_client_tagged_search_value,
    ltv_ad_clicks_current,
    ltv_search_with_ads_current,
    ltv_search_current,
    ltv_tagged_search_current,
    ltv_ad_clicks_future,
    ltv_search_with_ads_future,
    ltv_search_future,
    ltv_tagged_search_future
  )
FROM
  `moz-it-eip-revenue-users.ltv_derived.client_ltv_v1`
WHERE
  submission_date = @submission_date
