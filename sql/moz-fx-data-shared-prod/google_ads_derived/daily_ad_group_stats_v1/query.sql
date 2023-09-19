SELECT
  date_day AS `date`,
  * EXCEPT (date_day)
FROM
  `moz-fx-data-bq-fivetran.google_ads_google_ads.google_ads__ad_group_report`
