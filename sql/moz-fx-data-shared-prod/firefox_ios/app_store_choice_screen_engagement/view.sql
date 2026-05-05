CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.app_store_choice_screen_engagement`
AS
SELECT
  * EXCEPT (logical_date) REPLACE(DATE(`date`) AS `date`),
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.app_store_choice_screen_engagement_v1`
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      `date`,
      app_name,
      app_apple_identifier,
      event,
      device,
      platform_version,
      territory
    ORDER BY
      logical_date DESC
  ) = 1
