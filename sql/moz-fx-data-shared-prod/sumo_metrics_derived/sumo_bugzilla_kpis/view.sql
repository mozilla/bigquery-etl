CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.sumo_metrics_derived.sumo_bugzilla_kpis`
AS
WITH weeks AS (
  SELECT
    WEEK
  FROM
    UNNEST(
      GENERATE_DATE_ARRAY(
        DATE_TRUNC(DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR), WEEK(SUNDAY)),
        DATE_TRUNC(CURRENT_DATE, WEEK(SUNDAY)),
        INTERVAL 1 WEEK
      )
    ) AS week
),
weekly AS (
  SELECT
    DATE_TRUNC(`date`, WEEK(SUNDAY)) AS week,
    SUM(bugs_created) AS bugs_created,
    SUM(bugs_resolved) AS bugs_resolved,
  FROM
    `moz-fx-data-shared-prod.sumo_metrics_derived.bugzilla_tickets_base_v1`
  WHERE
    `date`
    BETWEEN DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)
    AND CURRENT_DATE
  GROUP BY
    WEEK
),
joined AS (
  SELECT
    weeks.week,
    COALESCE(weekly.bugs_created, 0) AS bugs_created,
    COALESCE(weekly.bugs_resolved, 0) AS bugs_resolved,
    COALESCE(weekly.bugs_created, 0) - COALESCE(weekly.bugs_resolved, 0) AS bug_inventory,
    SUM(COALESCE(weekly.bugs_created, 0) - COALESCE(weekly.bugs_resolved, 0)) OVER (
      ORDER BY
        weeks.week
    ) AS running_backlog,
  FROM
    weeks
  LEFT JOIN
    weekly
    USING (WEEK)
),
moving_avgs AS (
  SELECT
    *,
    AVG(bugs_created) OVER (
      ORDER BY
        WEEK
      ROWS BETWEEN
        5 PRECEDING
        AND CURRENT ROW
    ) AS bugs_created_6w_ma,
    AVG(bugs_resolved) OVER (
      ORDER BY
        WEEK
      ROWS BETWEEN
        5 PRECEDING
        AND CURRENT ROW
    ) AS bugs_resolved_6w_ma,
    AVG(bug_inventory) OVER (
      ORDER BY
        WEEK
      ROWS BETWEEN
        5 PRECEDING
        AND CURRENT ROW
    ) AS bug_inventory_6w_ma,
  FROM
    joined
)
SELECT
  *,
  LAG(bugs_created_6w_ma, 6) OVER (ORDER BY WEEK) AS lagged_bugs_created_6w_ma,
  LAG(bugs_resolved_6w_ma, 6) OVER (ORDER BY WEEK) AS lagged_bugs_resolved_6w_ma,
  LAG(bug_inventory_6w_ma, 6) OVER (ORDER BY WEEK) AS lagged_bug_inventory_6w_ma,
FROM
  moving_avgs
