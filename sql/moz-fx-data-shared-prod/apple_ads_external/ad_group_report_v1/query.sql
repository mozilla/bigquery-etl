SELECT
  date_day,
  organization_id,
  organization_name,
  campaign_id,
  campaign_name,
  ad_group_id,
  ad_group_name,
  currency,
  ad_group_status,
  start_at,
  end_at,
  taps,
  new_downloads,
  redownloads,
  total_downloads,
  impressions,
  spend,
FROM
  `moz-fx-data-bq-fivetran.dbt_fivetran_transformation_apple_search_ads.apple_search_ads__ad_group_report`