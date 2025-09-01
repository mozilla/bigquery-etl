CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_ios.app_store_funnel`
AS
-- v1 contains data based of the old report structure and v2 contains the data
-- based of the current report structure provided by the current Fivetran connector.
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.app_store_funnel_v1`
WHERE
  submission_date < "2024-04-01"
UNION ALL
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_ios_derived.app_store_funnel_v2`
WHERE
  submission_date >= "2024-04-01"
