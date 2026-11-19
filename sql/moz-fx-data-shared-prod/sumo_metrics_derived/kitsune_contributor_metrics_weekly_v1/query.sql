-- Weekly SUMO (Kitsune) contributor metrics, built on the answer-level fact
-- kitsune_contributor_answers_base_v1. Ported from the kitsune_weekly_contributors
-- dashboard query. Full-refresh daily: the `iso_week` series is anchored to the
-- full history of answers (not a trailing year), so each run reproduces the entire
-- weekly time series. Metrics are keyed on ISO week.
WITH main AS (
  SELECT
    answer_id,
    answer_date,
    creator_username
  FROM
    `moz-fx-data-shared-prod.sumo_metrics_derived.kitsune_contributor_answers_base_v1`
),
-- First answer per user based on the filtered base data.
filtered_first_answer_per_user AS (
  SELECT
    creator_username,
    MIN(answer_date) AS first_answer_date
  FROM
    main
  GROUP BY
    creator_username
),
-- Full history of ISO weeks, from the first answer week through the current week.
weeks AS (
  SELECT
    iso_week
  FROM
    UNNEST(
      GENERATE_DATE_ARRAY(
        (SELECT DATE_TRUNC(MIN(answer_date), ISOWEEK) FROM main),
        DATE_TRUNC(CURRENT_DATE(), ISOWEEK),
        INTERVAL 1 WEEK
      )
    ) AS iso_week
),
-- New contributors per week (based on filtered first answer date).
new_contributors AS (
  SELECT
    *,
    LAG(new_contributors_4w_ma, 4) OVER (ORDER BY iso_week) AS lagged_new_contributors_4w_ma
  FROM
    (
      SELECT
        w.iso_week,
        COUNT(DISTINCT f.creator_username) AS new_contributors,
        AVG(COUNT(DISTINCT f.creator_username)) OVER (
          ORDER BY
            w.iso_week
          ROWS BETWEEN
            3 PRECEDING
            AND CURRENT ROW
        ) AS new_contributors_4w_ma
      FROM
        weeks w
      LEFT JOIN
        filtered_first_answer_per_user f
        ON DATE_TRUNC(f.first_answer_date, ISOWEEK) = w.iso_week
      GROUP BY
        w.iso_week
    )
),
-- Top contributors: top 10 by answer count over the trailing 4 weeks of each week.
contributor_tiers AS (
  SELECT
    *,
    DENSE_RANK() OVER (PARTITION BY iso_week ORDER BY answers DESC) AS ranking,
    CASE
      WHEN DENSE_RANK() OVER (PARTITION BY iso_week ORDER BY answers DESC) <= 10
        THEN 'Top Contributor'
      ELSE 'Regular Contributor'
    END AS contributor_tier
  FROM
    (
      SELECT
        a.iso_week,
        m.creator_username,
        COUNT(m.answer_id) AS answers
      FROM
        (SELECT DISTINCT DATE_TRUNC(answer_date, ISOWEEK) AS iso_week FROM main) a
      JOIN
        main m
        ON DATE(m.answer_date) >= DATE_SUB(a.iso_week, INTERVAL 4 WEEK)
        AND DATE(m.answer_date) < DATE_ADD(a.iso_week, INTERVAL 1 WEEK)
      GROUP BY
        1,
        2
    )
),
main_with_tiers AS (
  SELECT
    main.*,
    contributor_tier
  FROM
    main
  LEFT JOIN
    contributor_tiers
    ON main.creator_username = contributor_tiers.creator_username
    AND DATE_TRUNC(main.answer_date, ISOWEEK) = contributor_tiers.iso_week
),
-- Share of weekly answers coming from Top Contributors.
top_contributor_perc AS (
  SELECT
    *,
    LAG(top_contributor_percentage_4w_ma, 4) OVER (
      ORDER BY
        iso_week
    ) AS lagged_top_contributor_percentage_4w_ma
  FROM
    (
      SELECT
        w.iso_week,
        SUM(CASE WHEN m.contributor_tier = 'Top Contributor' THEN 1 ELSE 0 END) / NULLIF(
          COUNT(m.answer_id),
          0
        ) AS top_contributor_percentage,
        AVG(
          SUM(CASE WHEN m.contributor_tier = 'Top Contributor' THEN 1 ELSE 0 END) / NULLIF(
            COUNT(m.answer_id),
            0
          )
        ) OVER (
          ORDER BY
            w.iso_week
          ROWS BETWEEN
            3 PRECEDING
            AND CURRENT ROW
        ) AS top_contributor_percentage_4w_ma
      FROM
        weeks w
      LEFT JOIN
        main_with_tiers m
        ON DATE_TRUNC(m.answer_date, ISOWEEK) = w.iso_week
      GROUP BY
        w.iso_week
    )
),
-- Pre-dedupe to (answer_date, creator_username, tier) for the retention range joins.
contributor_dates AS (
  SELECT DISTINCT
    answer_date,
    creator_username,
    contributor_tier
  FROM
    main_with_tiers
),
-- Contributor retention: % of the prior 4-week period's contributors still active
-- in the current 4-week period, broken out by their prior-period tier.
contributor_retention AS (
  SELECT
    *,
    LAG(retention_rate_4w_ma, 4) OVER (ORDER BY iso_week) AS lagged_retention_rate_4w_ma
  FROM
    (
      SELECT
        iso_week,
        SAFE_DIVIDE(retained_count, prev_count) AS retention_rate,
        AVG(SAFE_DIVIDE(retained_count, prev_count)) OVER (
          ORDER BY
            iso_week
          ROWS BETWEEN
            3 PRECEDING
            AND CURRENT ROW
        ) AS retention_rate_4w_ma,
        SAFE_DIVIDE(retained_count_top, prev_count_top) AS retention_rate_top,
        SAFE_DIVIDE(retained_count_regular, prev_count_regular) AS retention_rate_regular
      FROM
        (
          SELECT
            w.iso_week,
            COUNT(DISTINCT prev.creator_username) AS prev_count,
            COUNT(
              DISTINCT
              CASE
                WHEN curr.creator_username IS NOT NULL
                  THEN prev.creator_username
              END
            ) AS retained_count,
            COUNT(
              DISTINCT
              CASE
                WHEN prev.contributor_tier = 'Top Contributor'
                  THEN prev.creator_username
              END
            ) AS prev_count_top,
            COUNT(
              DISTINCT
              CASE
                WHEN prev.contributor_tier = 'Top Contributor'
                  AND curr.creator_username IS NOT NULL
                  THEN prev.creator_username
              END
            ) AS retained_count_top,
            COUNT(
              DISTINCT
              CASE
                WHEN prev.contributor_tier = 'Regular Contributor'
                  THEN prev.creator_username
              END
            ) AS prev_count_regular,
            COUNT(
              DISTINCT
              CASE
                WHEN prev.contributor_tier = 'Regular Contributor'
                  AND curr.creator_username IS NOT NULL
                  THEN prev.creator_username
              END
            ) AS retained_count_regular
          FROM
            weeks w
          LEFT JOIN
            contributor_dates prev
            ON prev.answer_date >= DATE_SUB(w.iso_week, INTERVAL 8 WEEK)
            AND prev.answer_date < DATE_SUB(w.iso_week, INTERVAL 4 WEEK)
          LEFT JOIN
            contributor_dates curr
            ON curr.creator_username = prev.creator_username
            AND curr.answer_date >= DATE_SUB(w.iso_week, INTERVAL 4 WEEK)
            AND curr.answer_date < DATE_ADD(w.iso_week, INTERVAL 1 WEEK)
          GROUP BY
            w.iso_week
        )
    )
),
-- Unique contributors who posted an answer in each specific week.
weekly_contributors AS (
  SELECT
    *,
    LAG(weekly_contributor_count_4w_ma, 4) OVER (
      ORDER BY
        iso_week
    ) AS lagged_weekly_contributor_count_4w_ma
  FROM
    (
      SELECT
        w.iso_week,
        COUNT(DISTINCT a.creator_username) AS weekly_contributor_count,
        AVG(COUNT(DISTINCT a.creator_username)) OVER (
          ORDER BY
            w.iso_week
          ROWS BETWEEN
            3 PRECEDING
            AND CURRENT ROW
        ) AS weekly_contributor_count_4w_ma
      FROM
        weeks w
      LEFT JOIN
        main a
        ON DATE_TRUNC(a.answer_date, ISOWEEK) = w.iso_week
      GROUP BY
        w.iso_week
    )
)
SELECT
  weekly_contributors.*,
  new_contributors.* EXCEPT (iso_week),
  top_contributor_perc.* EXCEPT (iso_week),
  contributor_retention.* EXCEPT (iso_week),
  CURRENT_TIMESTAMP() AS generated_time
FROM
  weekly_contributors
LEFT JOIN
  new_contributors
  ON weekly_contributors.iso_week = new_contributors.iso_week
LEFT JOIN
  top_contributor_perc
  ON weekly_contributors.iso_week = top_contributor_perc.iso_week
LEFT JOIN
  contributor_retention
  ON weekly_contributors.iso_week = contributor_retention.iso_week
