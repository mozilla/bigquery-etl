CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefoxdotcom.glean_desktop_conversion_events`
AS
-- Get all clicks from the countries without web cookie consent and the first session date associated with that click
WITH firefoxdotcom_sessions AS (
  SELECT
    gclid,
    MIN(session_date) AS first_session_date
  FROM
    `moz-fx-data-shared-prod.firefoxdotcom_derived.ga_sessions_v2` AS ga_sessions_v2,
    UNNEST(gclid_array) AS gclid
  LEFT JOIN
    `moz-fx-data-shared-prod.static.country_names_v1` AS country_names
    ON ga_sessions_v2.country = country_names.name
  LEFT JOIN
    `moz-fx-data-shared-prod.static.marketing_country_tier_mapping_v1` AS marketing_tier_mapping
    ON country_names.code = marketing_tier_mapping.country_code
  WHERE
    NOT COALESCE(marketing_tier_mapping.has_web_cookie_consent, FALSE)
  GROUP BY
    gclid
)
-- Get all conversion events and associated clicks in the last 28 days
-- where the click ID originated from a country without a web cookie consent
-- and the click's first seen session date is more recent than 28 days ago
SELECT
  gclid,
  conversion_name,
  FORMAT_TIMESTAMP(
    "%Y-%m-%d %X %EZ",
    CAST(MIN(activity_datetime) AS TIMESTAMP)
  ) AS activity_date_timestamp
FROM
  `moz-fx-data-shared-prod.firefoxdotcom_derived.glean_ga_desktop_conversions_v1`
JOIN
  firefoxdotcom_sessions
  USING (gclid)
WHERE
  first_session_date >= DATE_SUB(CURRENT_DATE, INTERVAL 28 day)
GROUP BY
  gclid,
  conversion_name
HAVING
  MIN(activity_date) >= DATE_SUB(CURRENT_DATE, INTERVAL 28 DAY)
