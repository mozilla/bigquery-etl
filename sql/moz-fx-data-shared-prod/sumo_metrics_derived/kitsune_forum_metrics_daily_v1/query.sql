-- Identify answers from deactivated users so they can be excluded from TTFR
-- and helpfulness vote calculations, matching kitsune_forum_posts.sql logic.
WITH deactivation_filter AS (
  SELECT
    a.answer_id,
    IF(u.is_active IS FALSE, TRUE, FALSE) AS is_deactivated
  FROM
    `moz-fx-sumo-prod.sumo.kitsune_answers_raw` a
  LEFT JOIN
    `moz-fx-sumo-prod.sumo.kitsune_auth_user` u
    ON a.creator_username = u.username
  GROUP BY
    a.answer_id,
    u.is_active
),
-- First non-OP, non-spam reply from an active user; used for TTFR and reply rate.
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
        `moz-fx-sumo-prod.sumo.kitsune_questions_plus` q
      JOIN
        `moz-fx-sumo-prod.sumo.kitsune_answers_raw` fr
        ON q.question_id = fr.question_id
      LEFT JOIN
        deactivation_filter df
        ON df.answer_id = fr.answer_id
      WHERE
        fr.creator_username <> q.creator_username
        AND fr.is_spam IS NOT TRUE
        AND df.is_deactivated IS NOT TRUE
    )
  WHERE
    rn = 1
),
-- Cumulative helpful/unhelpful votes per question, excluding OP answers, spam,
-- and answers from deactivated users.
helpful_votes AS (
  SELECT
    a.question_id,
    SUM(a.num_helpful_votes) AS total_helpful_votes,
    SUM(a.num_unhelpful_votes) AS total_unhelpful_votes
  FROM
    `moz-fx-sumo-prod.sumo.kitsune_answers_raw` a
  LEFT JOIN
    `moz-fx-sumo-prod.sumo.kitsune_questions_plus` q
    ON a.question_id = q.question_id
  LEFT JOIN
    deactivation_filter df
    ON df.answer_id = a.answer_id
  WHERE
    a.creator_username != q.creator_username
    AND a.is_spam IS NOT TRUE
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
    COALESCE(hv.total_helpful_votes, 0) AS total_helpful_votes,
    COALESCE(hv.total_unhelpful_votes, 0) AS total_unhelpful_votes
  FROM
    `moz-fx-sumo-prod.sumo.kitsune_questions_plus` q
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
),
events AS (
  SELECT
    created_date AS `date`,
    product,
    locale,
    'created' AS event_type,
    NULL AS ttfr_mins,
    total_helpful_votes,
    total_unhelpful_votes
  FROM
    questions
  WHERE
    created_date IS NOT NULL
  UNION ALL
  SELECT
    first_reply_date AS `date`,
    product,
    locale,
    'replied' AS event_type,
    ttfr_mins,
    0 AS total_helpful_votes,
    0 AS total_unhelpful_votes
  FROM
    questions
  WHERE
    first_reply_date IS NOT NULL
)
SELECT
  `date`,
  product,
  locale,
  COUNTIF(event_type = 'created') AS questions_created,
  COUNTIF(event_type = 'replied') AS questions_replied,
  AVG(IF(event_type = 'replied', ttfr_mins / 60.0, NULL)) AS avg_ttfr_hrs,
  SUM(IF(event_type = 'created', total_helpful_votes, 0)) AS total_helpful_votes,
  SUM(IF(event_type = 'created', total_unhelpful_votes, 0)) AS total_unhelpful_votes,
  CURRENT_TIMESTAMP() AS etl_timestamp
FROM
  events
GROUP BY
  `date`,
  product,
  locale
ORDER BY
  `date`,
  product,
  locale
