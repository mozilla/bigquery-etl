-- This is an authorized view that allows us to read Apple Ads data
-- from a Fivetran project.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.apple_ads.keyword_report`
AS
SELECT
  *
FROM
  `moz-fx-data-bq-fivetran.dbt_fivetran_transformation_apple_search_ads.apple_search_ads__keyword_report`
