WITH cs_impressions AS (
  SELECT
    country AS country_code,
    SUM(CASE WHEN position <= 2 THEN event_count END) AS sponsored_impressions_1and2,
    SUM(event_count) AS sponsored_impressions_all
  FROM
    `moz-fx-data-shared-prod.contextual_services.event_aggregates`
  WHERE
    event_type = 'impression'
    AND form_factor = 'desktop'
    AND source = 'topsites'
    AND (
      submission_date >= DATE_TRUNC(DATE_SUB(@submission_date, INTERVAL 1 MONTH), MONTH)
      AND submission_date < DATE_TRUNC(@submission_date, MONTH)
    )
    AND country IN ('US', 'DE', 'FR', 'AU', 'CA', 'IT', 'ES', 'MX', 'BR', 'IN', 'GB', 'JP')
  GROUP BY
    1
)
/* Remnant of old telemetry, which was formally deprecated in February 2024 */
,
as_sessions AS (
  SELECT
    normalized_country_code AS country_code,
    APPROX_COUNT_DISTINCT(session_id) AS as_sessions,
    2 * APPROX_COUNT_DISTINCT(session_id) AS sessions_total_inventory_1and2,
    APPROX_COUNT_DISTINCT(client_id) AS session_total_clients
  FROM
    `mozdata.activity_stream.sessions`
  WHERE
    (
      DATE(submission_timestamp) >= DATE_TRUNC(DATE_SUB(@submission_date, INTERVAL 1 MONTH), MONTH)
      AND DATE(submission_timestamp) < DATE_TRUNC(@submission_date, MONTH)
    )
    AND normalized_country_code IN (
      'US',
      'DE',
      'FR',
      'AU',
      'CA',
      'IT',
      'ES',
      'MX',
      'BR',
      'IN',
      'GB',
      'JP'
    )
    AND page IN ('about:home', 'about:newtab')
    AND (mozfun.norm.browser_version_info(version).major_version >= 91 AND user_prefs >= 258)
  GROUP BY
    1
)
/* Deriving total users from unified_metrics given how the DAU forecast doesn't account for NT activity data */
,
users_table AS (
  SELECT
    country AS country_code,
    COUNT(client_id) AS total_user_count
  FROM
    `mozdata.telemetry.unified_metrics`
  WHERE
    `mozfun`.bits28.active_in_range(days_seen_bits, 0, 1)
    AND (
      submission_date >= DATE_TRUNC(DATE_SUB(@submission_date, INTERVAL 1 MONTH), MONTH)
      AND submission_date < DATE_TRUNC(@submission_date, MONTH)
    )
    AND country IN ('US', 'DE', 'FR', 'AU', 'CA', 'IT', 'ES', 'MX', 'BR', 'IN', 'GB', 'JP')
    AND normalized_app_name = 'Firefox Desktop'
  GROUP BY
    1
)
/* Using 2x visits as total inventory while we sort out addressable inventory for eligible users */
,
nt_visits AS (
  SELECT
    DATE_TRUNC(DATE_SUB(@submission_date, INTERVAL 1 MONTH), MONTH) AS submission_month,
    n.country_code,
    APPROX_COUNT_DISTINCT(newtab_visit_id) AS newtab_visits,
    APPROX_COUNT_DISTINCT(client_id) AS newtab_clients,
    2 * APPROX_COUNT_DISTINCT(newtab_visit_id) AS visits_total_inventory_1and2,
    3 * APPROX_COUNT_DISTINCT(newtab_visit_id) AS visits_total_inventory_3,
    SUM(t.sponsored_topsite_tile_impressions) AS sponsored_impressions
  FROM
    `moz-fx-data-shared-prod.telemetry.newtab_visits` n,
    UNNEST(topsite_tile_interactions) t
  WHERE
    n.topsites_enabled
    AND n.topsites_sponsored_enabled
    AND (
      submission_date >= DATE_TRUNC(DATE_SUB(@submission_date, INTERVAL 1 MONTH), MONTH)
      AND submission_date < DATE_TRUNC(@submission_date, MONTH)
    )
    AND n.country_code IN ('US', 'DE', 'FR', 'AU', 'CA', 'IT', 'ES', 'MX', 'BR', 'IN', 'GB', 'JP')
  GROUP BY
    1,
    2
)
SELECT
  n.submission_month,
  n.country_code AS country,
  u.total_user_count,
  c.sponsored_impressions_1and2,
  c.sponsored_impressions_all,
  n.newtab_visits,
  n.newtab_clients,
  n.visits_total_inventory_1and2,
  ROUND(
    1.00 * c.sponsored_impressions_1and2 / n.visits_total_inventory_1and2,
    3
  ) AS visits_total_fill_rate_1and2,
  n.visits_total_inventory_3,
  ROUND(
    1.00 * c.sponsored_impressions_all / n.visits_total_inventory_3,
    3
  ) AS visits_total_fill_rate_3,
  a.as_sessions,
  a.sessions_total_inventory_1and2,
  ROUND(
    1.00 * c.sponsored_impressions_1and2 / a.sessions_total_inventory_1and2,
    3
  ) AS sessions_total_fill_rate_1and2
FROM
  nt_visits n
LEFT JOIN
  cs_impressions AS c
  ON c.country_code = n.country_code
LEFT JOIN
  users_table u
  ON u.country_code = n.country_code
LEFT JOIN
  as_sessions a
  ON a.country_code = n.country_code
ORDER BY
  2 ASC
