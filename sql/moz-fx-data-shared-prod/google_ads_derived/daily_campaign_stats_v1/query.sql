SELECT
  date_day AS `date`,
  * EXCEPT (date_day)
FROM
  `moz-fx-data-bq-fivetran.ads_google_mmc_google_ads.google_ads__campaign_report`
