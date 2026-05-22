-- Identify answers from deactivated users so they can be excluded from
-- helpfulness vote calculations, matching kitsune_forum_posts.sql logic.
WITH deactivation_filter AS (
  SELECT
    a.answer_id,
    IF(u.is_active IS FALSE, TRUE, FALSE) AS is_deactivated
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_answers_raw` a
  LEFT JOIN
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_auth_user` u
    ON a.creator_username = u.username
  GROUP BY
    a.answer_id,
    u.is_active
),
-- First non-OP, non-spam reply; used for TTFR and reply rate.
-- Matches kitsune_forum_posts.sql: deactivated-user answers are NOT excluded here.
time_to_first_reply AS (
  SELECT
    question_id,
    DATE(reply_date) AS first_reply_date,
    TIMESTAMP_DIFF(reply_date, created_date, MINUTE) AS ttfr_mins
  FROM
    (
      SELECT
        q.question_id,
        q.created_date,
        fr.created_date AS reply_date,
        ROW_NUMBER() OVER (PARTITION BY fr.question_id ORDER BY fr.created_date) AS rn
      FROM
        `moz-fx-data-shared-prod.sumo_syndicate.kitsune_questions_plus` q
      LEFT JOIN
        `moz-fx-data-shared-prod.sumo_syndicate.kitsune_answers_raw` fr
        ON q.question_id = fr.question_id
      WHERE
        fr.creator_username <> q.creator_username
        AND fr.is_spam IS NOT TRUE
    )
  WHERE
    rn = 1
),
-- Cumulative helpful/unhelpful votes per question, excluding spam answers
-- and answers from deactivated users. OP self-answers are included.
helpful_votes AS (
  SELECT
    a.question_id,
    SUM(a.num_helpful_votes) AS total_helpful_votes,
    SUM(a.num_unhelpful_votes) AS total_unhelpful_votes
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_answers_raw` a
  LEFT JOIN
    deactivation_filter df
    ON df.answer_id = a.answer_id
  WHERE
    a.is_spam IS NOT TRUE
    AND df.is_deactivated IS NOT TRUE
  GROUP BY
    a.question_id
),
questions AS (
  SELECT
    DATE(q.created_date) AS created_date,
    CASE
      q.product
      WHEN 'firefox'
        THEN 'Firefox desktop'
      WHEN 'mobile'
        THEN 'Firefox for Android'
      WHEN 'ios'
        THEN 'Firefox for iOS'
      WHEN 'firefox-enterprise'
        THEN 'Firefox for Enterprise'
    END AS product,
    q.locale,
    ttfr.first_reply_date,
    ttfr.ttfr_mins,
    COALESCE(q.is_solved, FALSE) AS is_solved,
    DATE(q.solved_date) AS solved_date,
    COALESCE(hv.total_helpful_votes, 0) AS total_helpful_votes,
    COALESCE(hv.total_unhelpful_votes, 0) AS total_unhelpful_votes
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_questions_plus` q
  LEFT JOIN
    time_to_first_reply ttfr
    ON q.question_id = ttfr.question_id
  LEFT JOIN
    helpful_votes hv
    ON q.question_id = hv.question_id
  WHERE
    q.is_spam = FALSE
    AND q.is_locked = FALSE
    AND q.product IN ('firefox', 'mobile', 'ios', 'firefox-enterprise')
)
SELECT
  created_date,
  first_reply_date,
  solved_date,
  product,
  locale,
  COUNT(*) AS questions_created,
  COUNTIF(first_reply_date IS NOT NULL) AS questions_replied,
  COUNTIF(is_solved) AS questions_solved,
  AVG(ttfr_mins / 60.0) AS avg_ttfr_hrs,
  SUM(ttfr_mins / 60.0) AS total_ttfr_hrs,
  SUM(total_helpful_votes) AS total_helpful_votes,
  SUM(total_unhelpful_votes) AS total_unhelpful_votes,
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM
  questions
WHERE
  created_date IS NOT NULL
GROUP BY
  created_date,
  first_reply_date,
  solved_date,
  product,
  locale
ORDER BY
  created_date,
  first_reply_date,
  solved_date,
  product,
  locale
