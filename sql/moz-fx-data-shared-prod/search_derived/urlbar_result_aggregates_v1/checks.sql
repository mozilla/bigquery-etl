#fail
{{ min_row_count(1, "submission_date = @submission_date") }}

#fail
-- Every measure must sum exactly to urlbar_clients_daily_v1: every client's results are
-- kept, with or without a dims row.
WITH
AGGREGATE
  AS (
    SELECT
      SUM(clicks) AS clicks,
      SUM(sessions_shown) AS sessions_shown,
      SUM(sessions_shown_abandoned) AS sessions_shown_abandoned,
      SUM(annoyances) AS annoyances
    FROM
      `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
    WHERE
      submission_date = @submission_date
  ),
  source AS (
    SELECT
      SUM(r.clicks) AS clicks,
      SUM(r.sessions_shown) AS sessions_shown,
      SUM(r.sessions_shown_abandoned) AS sessions_shown_abandoned,
      SUM(r.annoyances) AS annoyances
    FROM
      `{{ project_id }}.{{ dataset_id }}.urlbar_clients_daily_v1`
    CROSS JOIN
      UNNEST(results) AS r
    WHERE
      submission_date = @submission_date
  )
SELECT
  IF(
    a.clicks IS DISTINCT FROM s.clicks
    OR a.sessions_shown IS DISTINCT FROM s.sessions_shown
    OR a.sessions_shown_abandoned IS DISTINCT FROM s.sessions_shown_abandoned
    OR a.annoyances IS DISTINCT FROM s.annoyances,
    ERROR(
      FORMAT(
        "clicks, sessions_shown, sessions_shown_abandoned, annoyances are (%d, %d, %d, %d) here but (%d, %d, %d, %d) in urlbar_clients_daily_v1",
        a.clicks,
        a.sessions_shown,
        a.sessions_shown_abandoned,
        a.annoyances,
        s.clicks,
        s.sessions_shown,
        s.sessions_shown_abandoned,
        s.annoyances
      )
    ),
    NULL
  )
FROM
AGGREGATE
  AS a,
  source AS s;
