WITH cs_impressions AS (
  SELECT
    country AS country_code,
    DATE_TRUNC(submission_date, MONTH) AS submission_month,
    SUM(IF(position <= 2, event_count, 0)) AS sponsored_impressions_1and2,
    SUM(event_count) AS sponsored_impressions_all
  FROM
    `moz-fx-data-shared-prod.contextual_services.event_aggregates`
  WHERE
    event_type = 'impression'
    AND form_factor = 'desktop'
    AND source = 'topsites'
    AND (
      {% if is_init() %}
        submission_date >= DATE_TRUNC(PARSE_DATE('%Y-%m-%d', '2023-11-01'), MONTH)
      {% else %}
        submission_date >= DATE_TRUNC(DATE_SUB(@submission_month, INTERVAL 1 MONTH), MONTH)
      {% endif %}
    )
    AND submission_date < @submission_month
    AND country IN ('US', 'DE', 'FR', 'AU', 'CA', 'IT', 'ES', 'MX', 'BR', 'IN', 'GB', 'JP')
  GROUP BY
    country_code,
    submission_month
)
/* Deriving total users from unified_metrics given how the DAU forecast doesn't account for NT activity data */
,
users_table AS (
  SELECT
    country AS country_code,
    DATE_TRUNC(submission_date, MONTH) AS submission_month,
    COUNT(client_id) AS total_user_count
  FROM
    `mozdata.telemetry.unified_metrics`
  WHERE
    `mozfun`.bits28.active_in_range(days_seen_bits, 0, 1)
    AND (
      {% if is_init() %}
        submission_date >= DATE_TRUNC(PARSE_DATE('%Y-%m-%d', '2023-11-01'), MONTH)
      {% else %}
        submission_date >= DATE_TRUNC(DATE_SUB(@submission_month, INTERVAL 1 MONTH), MONTH)
      {% endif %}
    )
    AND submission_date < @submission_month
    AND country IN ('US', 'DE', 'FR', 'AU', 'CA', 'IT', 'ES', 'MX', 'BR', 'IN', 'GB', 'JP')
    AND normalized_app_name = 'Firefox Desktop'
  GROUP BY
    submission_month,
    country_code
)
/* Using 2x visits as total inventory while we sort out addressable inventory for eligible users */
,
nt_visits AS (
  SELECT
    DATE_TRUNC(submission_date, MONTH) AS submission_month,
    n.country_code,
    APPROX_COUNT_DISTINCT(newtab_visit_id) AS newtab_visits,
    APPROX_COUNT_DISTINCT(client_id) AS newtab_clients,
    2 * APPROX_COUNT_DISTINCT(newtab_visit_id) AS visits_total_inventory_1and2,
    3 * APPROX_COUNT_DISTINCT(newtab_visit_id) AS visits_total_inventory_1to3,
    SUM(t.sponsored_topsite_tile_impressions) AS sponsored_impressions
  FROM
    `moz-fx-data-shared-prod.telemetry.newtab_visits` n,
    UNNEST(topsite_tile_interactions) t
  WHERE
    n.topsites_enabled
    AND n.topsites_sponsored_enabled
    AND (
      {% if is_init() %}
        submission_date >= DATE_TRUNC(PARSE_DATE('%Y-%m-%d', '2023-11-01'), MONTH)
      {% else %}
        submission_date >= DATE_TRUNC(DATE_SUB(@submission_month, INTERVAL 1 MONTH), MONTH)
      {% endif %}
    )
    AND submission_date < @submission_month
    AND n.country_code IN ('US', 'DE', 'FR', 'AU', 'CA', 'IT', 'ES', 'MX', 'BR', 'IN', 'GB', 'JP')
  GROUP BY
    submission_month,
    country_code
)
SELECT
  n.submission_month,
  n.country_code AS country,
  u.total_user_count AS user_count,
  c.sponsored_impressions_1and2 AS impression_count_1and2,
  c.sponsored_impressions_all,
  n.newtab_visits AS visit_count,
  n.newtab_clients AS clients,
  n.visits_total_inventory_1and2 AS total_inventory_1and2,
  ROUND(1.00 * c.sponsored_impressions_1and2 / n.visits_total_inventory_1and2, 3) AS fill_rate,
  n.visits_total_inventory_1to3,
  ROUND(
    1.00 * c.sponsored_impressions_all / n.visits_total_inventory_1to3,
    3
  ) AS visits_total_fill_rate_1to3,
FROM
  nt_visits n
LEFT JOIN
  cs_impressions AS c
  ON c.country_code = n.country_code
  AND c.submission_month = n.submission_month
LEFT JOIN
  users_table u
  ON u.country_code = n.country_code
  AND u.submission_month = n.submission_month
ORDER BY
  country,
  submission_month
