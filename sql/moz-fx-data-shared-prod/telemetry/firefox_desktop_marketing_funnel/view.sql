CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.firefox_desktop_marketing_funnel`
AS
SELECT
  marketing_funnel.*,
  tier_mapping.tier AS country_tier,
FROM
  `moz-fx-data-shared-prod.telemetry_derived.firefox_desktop_marketing_funnel_v1` AS marketing_funnel
LEFT JOIN
  `moz-fx-data-shared-prod.static.marketing_country_tier_mapping_v1` AS tier_mapping
  ON marketing_funnel.country = tier_mapping.country_code
WHERE
  (NOT COALESCE(tier_mapping.has_web_cookie_consent, FALSE) OR funnel_derived = 'partner')
