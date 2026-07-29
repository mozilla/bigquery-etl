-- Answer-level contributor fact for the SUMO Kitsune support forum.
-- One row per qualifying answer: non-spam, posted by an active user, on a
-- Firefox product, excluding OP self-answers. Mirrors the `main` CTE of the
-- kitsune_weekly_contributors dashboard query and is the reusable base on top
-- of which kitsune_contributor_metrics_weekly_v1 is built.
WITH questions AS (
  SELECT
    q.question_id,
    DATE(q.created_date) AS question_date,
    -- Canonicalize Firefox product names; non-Firefox products are excluded.
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
    q.creator_username AS question_creator_username,
    q.num_replies,
    q.num_replies_from_others
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_questions_plus` q
  WHERE
    q.product IN ('firefox', 'mobile', 'ios', 'firefox-enterprise')
),
-- Last reply timestamp per question across ALL answers (unfiltered), matching
-- the dashboard's MAX(created_date) OVER (PARTITION BY question_id).
last_reply_per_question AS (
  SELECT
    question_id,
    MAX(created_date) AS last_reply_on_question
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_answers_raw`
  GROUP BY
    question_id
),
-- First/last answer dates per user across ALL their answers (unfiltered:
-- includes spam, inactive, non-Firefox and OP self-answers).
user_answer_span AS (
  SELECT
    creator_username,
    MIN(DATE(created_date)) AS user_first_answer_date,
    MAX(DATE(created_date)) AS user_last_answer_date
  FROM
    `moz-fx-data-shared-prod.sumo_syndicate.kitsune_answers_raw`
  GROUP BY
    creator_username
)
SELECT
  a.answer_id,
  DATE(a.created_date) AS answer_date,
  a.creator_username,
  CONCAT('https://support.mozilla.org/user/', a.creator_username) AS creator_profile,
  u.email,
  a.question_id,
  q.question_date,
  CONCAT(
    'https://support.mozilla.org/',
    q.locale,
    '/questions/',
    CAST(a.question_id AS STRING),
    '#answer-',
    CAST(a.answer_id AS STRING)
  ) AS answer_url,
  q.product,
  q.locale,
  q.num_replies_from_others,
  lr.last_reply_on_question,
  IF(q.num_replies > q.num_replies_from_others, 1, 0) AS op_reply_back,
  a.num_helpful_votes AS total_helpful_votes,
  a.num_unhelpful_votes AS total_unhelpful_votes,
  a.num_helpful_votes + a.num_unhelpful_votes AS total_helpfulness,
  s.user_first_answer_date,
  s.user_last_answer_date,
  DATE(u.date_joined) AS date_joined,
  DATE(u.last_login) AS last_login,
  CURRENT_TIMESTAMP() AS generated_time
FROM
  `moz-fx-data-shared-prod.sumo_syndicate.kitsune_answers_raw` a
JOIN
  questions q
  ON a.question_id = q.question_id
LEFT JOIN
  `moz-fx-data-shared-prod.sumo_syndicate.kitsune_auth_user` u
  ON a.creator_username = u.username
LEFT JOIN
  last_reply_per_question lr
  ON a.question_id = lr.question_id
LEFT JOIN
  user_answer_span s
  ON a.creator_username = s.creator_username
WHERE
  a.is_spam IS NOT TRUE
  AND u.is_active IS TRUE
  -- Exclude OP self-answers (keep when creators differ or OP is unknown),
  -- matching is_answer_by_op = 0 in the dashboard query.
  AND COALESCE(a.creator_username = q.question_creator_username, FALSE) IS NOT TRUE
