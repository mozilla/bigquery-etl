CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_org.desktop_conversion_events`
AS
-- Get all clicks from the US and the first session date associated with that click
WITH all_clicks_from_united_states AS (
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
    c.name = 'United States'
  GROUP BY
    gclid
)
--Get all conversion events and associated clicks in the last 28 days
--where the click ID originated in the US
--and the click's first seen session date is more recent than 28 days ago
SELECT
  a.gclid,
  a.conversion_name,
  FORMAT_TIMESTAMP(
    "%Y-%m-%d %X %EZ",
    CAST(MIN(a.activity_datetime) AS TIMESTAMP)
  ) AS activity_date_timestamp
FROM
  `moz-fx-data-shared-prod.mozilla_org_derived.ga_desktop_conversions_v2` a
JOIN
  all_clicks_from_united_states b
  ON a.gclid = b.gclid
WHERE
  b.first_session_date >= DATE_SUB(current_date, INTERVAL 28 day)
GROUP BY
  a.gclid,
  a.conversion_name
HAVING
  MIN(a.activity_date) >= DATE_SUB(CURRENT_DATE, INTERVAL 28 DAY)
