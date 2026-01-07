SELECT
  date_day AS `date`,
  * EXCEPT (date_day),
  @submission_date AS last_updated_date
FROM
  `moz-fx-data-shared-prod.google_ads_syndicate.google_ads__campaign_report`
