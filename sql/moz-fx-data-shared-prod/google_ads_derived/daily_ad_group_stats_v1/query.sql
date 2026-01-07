SELECT
  date_day AS `date`,
  * EXCEPT (date_day)
FROM
  `moz-fx-data-shared-prod.google_ads_syndicate.google_ads__ad_group_report`
