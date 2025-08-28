SELECT
  date_day,
  organization_id,
  organization_name,
  currency,
  taps,
  tap_new_downloads AS new_downloads,
  tap_redownloads AS redownloads,
  total_downloads,
  impressions,
  spend,
FROM
  `moz-fx-data-bq-fivetran.dbt_fivetran_transformation_apple_search_ads.apple_search_ads__organization_report`
