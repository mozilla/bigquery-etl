CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.sumo_metrics_derived.sumo_search_success_kpis`
AS
-- Daily KB search success rate (SSR) for SUMO, by search locale, plus an
-- overall day-level 14-day moving average and its 14-day-lagged comparison.
-- Built from the session-grain base table so the success definition lives in
-- one place.
WITH daily_search_success_counts AS (
  SELECT
    session_date,
    search_locale,
    COUNT(DISTINCT CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING))) AS search_sessions,
    COUNT(
      DISTINCT IF(
        is_successful_search = 1,
        CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING)),
        NULL
      )
    ) AS successful_search_sessions,
    COUNT(
      DISTINCT IF(
        is_successful_search_1min = 1,
        CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING)),
        NULL
      )
    ) AS successful_search_sessions_1min,
    COUNT(
      DISTINCT IF(
        is_successful_search_2min = 1,
        CONCAT(user_pseudo_id, '-', CAST(ga_session_id AS STRING)),
        NULL
      )
    ) AS successful_search_sessions_2min
  FROM
    `moz-fx-data-shared-prod.sumo_metrics_derived.ga4_search_success_sessions_base_v1`
  GROUP BY
    session_date,
    search_locale
),
-- Overall (all-locale) daily SSR with a 14-day moving average and lag.
day_level AS (
  SELECT
    *,
    LAG(total_ssr_14_ma, 14) OVER (ORDER BY session_date) AS lagged_total_ssr_14_ma
  FROM
    (
      SELECT
        *,
        AVG(total_ssr) OVER (
          ORDER BY
            session_date
          ROWS BETWEEN
            13 PRECEDING
            AND CURRENT ROW
        ) AS total_ssr_14_ma
      FROM
        (
          SELECT
            session_date,
            SUM(successful_search_sessions) AS total_successful_search_sessions,
            SUM(search_sessions) AS total_search_sessions,
            SAFE_DIVIDE(SUM(successful_search_sessions), SUM(search_sessions)) AS total_ssr
          FROM
            daily_search_success_counts
          GROUP BY
            session_date
        )
    )
)
SELECT
  c.*,
  SAFE_DIVIDE(c.successful_search_sessions, c.search_sessions) AS ssr,
  SAFE_DIVIDE(c.successful_search_sessions_1min, c.search_sessions) AS ssr_1min,
  SAFE_DIVIDE(c.successful_search_sessions_2min, c.search_sessions) AS ssr_2min,
  d.* EXCEPT (session_date)
FROM
  daily_search_success_counts c
LEFT JOIN
  day_level d
  ON c.session_date = d.session_date
