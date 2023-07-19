CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.clients_first_seen`
AS
SELECT
  * EXCEPT (search_with_ads_combined_sum, ad_clicks_combined_sum) REPLACE(
    CASE
      WHEN mozfun.norm.truncate_version(app_version, "major") <= 108
        THEN search_with_ads
      WHEN mozfun.norm.truncate_version(app_version, "major") > 108
        THEN search_with_ads_combined_sum
      ELSE NULL
    END AS search_with_ads,
    CASE
      WHEN `mozfun.norm.truncate_version`(app_version, "major") <= 108
        THEN ad_clicks
      WHEN `mozfun.norm.truncate_version`(app_version, "major") > 108
        THEN ad_clicks_combined_sum
      ELSE NULL
    END AS ad_clicks
  )
FROM
  `moz-fx-data-shared-prod.telemetry_derived.clients_first_seen_v1`
