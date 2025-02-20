CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.app_store_choice_screen_selection`
AS
SELECT
  * EXCEPT (logical_date) REPLACE(DATE(`date`) AS `date`),
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.app_store_choice_screen_selection_v1`
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      `date`,
      territory,
      build,
      device,
      platform,
      release_type
    ORDER BY
      logical_date DESC
  ) = 1
