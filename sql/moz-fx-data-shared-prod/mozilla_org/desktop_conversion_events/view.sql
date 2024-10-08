CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_org.desktop_conversion_events`
AS
-- Get all clicks not originating from Europe and the first session date associated with that click
WITH all_clicks_not_originating_in_europe AS (
  SELECT
    gclid,
    MIN(session_date) AS first_session_date
  FROM
    `moz-fx-data-shared-prod.mozilla_org_derived.ga_sessions_v2` AS ga_sessions_v2,
    UNNEST(gclid_array) AS gclid
  JOIN
    `moz-fx-data-shared-prod.static.country_codes_v1` c
    ON ga_sessions_v2.country = c.name
  WHERE
    region_name != 'Europe'
  GROUP BY
    gclid
)
--Get all conversion events and associated clicks in the last 89 days
--where the click ID did not originate in Europe
--and the click's first seen session date is more recent than 89 days ago
SELECT
  a.gclid,
  a.conversion_name,
  MIN(a.activity_datetime) AS activity_date,
FROM
  `moz-fx-data-shared-prod.mozilla_org_derived.ga_desktop_conversions_v1` a
JOIN
  all_clicks_not_originating_in_europe b
  ON a.gclid = b.gclid
WHERE
  b.first_session_date >= DATE_SUB(current_date, INTERVAL 89 day)
GROUP BY
  a.gclid,
  a.conversion_name
HAVING
  MIN(a.activity_date) >= DATE_SUB(CURRENT_DATE, INTERVAL 89 DAY)
