SELECT
  date_day,
  organization_id,
  organization_name,
  campaign_id,
  campaign_name,
  currency,
  campaign_status,
  start_at,
  end_at,
  taps,
  new_downloads,
  redownloads,
  total_downloads,
  impressions,
  spend,
FROM
  `moz-fx-data-bq-fivetran.dbt_fivetran_transformation_apple_search_ads.apple_search_ads__campaign_report`