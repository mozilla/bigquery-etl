-- extract the relevant fields for each funnel step and segment if necessary
WITH onboarding_funnel_first_card AS (
  SELECT
    client_info.client_id AS join_key,
    `mozfun.map.get_key`(event_extra, 'sequence_id') AS funnel_id,
    `mozfun.map.get_key`(event_extra, 'action') AS action,
    `mozfun.map.get_key`(event_extra, 'element_type') AS element_type,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    fenix.events_unnested
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND `mozfun.map.get_key`(event_extra, 'sequence_position') = '1'
    AND event_name != 'completed'
    AND event_category = 'onboarding'
),
onboarding_funnel_second_card AS (
  SELECT
    client_info.client_id AS join_key,
    prev.funnel_id AS funnel_id,
    prev.action AS action,
    prev.element_type AS element_type,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    fenix.events_unnested
  INNER JOIN
    onboarding_funnel_first_card AS prev
  ON
    prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND `mozfun.map.get_key`(event_extra, 'sequence_position') = '2'
    AND event_name != 'completed'
    AND event_category = 'onboarding'
),
onboarding_funnel_third_card AS (
  SELECT
    client_info.client_id AS join_key,
    prev.funnel_id AS funnel_id,
    prev.action AS action,
    prev.element_type AS element_type,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    fenix.events_unnested
  INNER JOIN
    onboarding_funnel_second_card AS prev
  ON
    prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND `mozfun.map.get_key`(event_extra, 'sequence_position') = '3'
    AND event_name != 'completed'
    AND event_category = 'onboarding'
),
onboarding_funnel_onboarding_completed AS (
  SELECT
    client_info.client_id AS join_key,
    prev.funnel_id AS funnel_id,
    prev.action AS action,
    prev.element_type AS element_type,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    fenix.events_unnested
  INNER JOIN
    onboarding_funnel_third_card AS prev
  ON
    prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_name = 'completed'
    AND event_category = 'onboarding'
),
-- aggregate each funnel step value
onboarding_funnel_first_card_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
    action,
    element_type,
    COUNT(DISTINCT column) AS aggregated
  FROM
    onboarding_funnel_first_card
  GROUP BY
    funnel_id,
    action,
    element_type,
    submission_date,
    funnel
),
onboarding_funnel_second_card_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
    action,
    element_type,
    COUNT(DISTINCT column) AS aggregated
  FROM
    onboarding_funnel_second_card
  GROUP BY
    funnel_id,
    action,
    element_type,
    submission_date,
    funnel
),
onboarding_funnel_third_card_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
    action,
    element_type,
    COUNT(DISTINCT column) AS aggregated
  FROM
    onboarding_funnel_third_card
  GROUP BY
    funnel_id,
    action,
    element_type,
    submission_date,
    funnel
),
onboarding_funnel_onboarding_completed_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
    action,
    element_type,
    COUNT(DISTINCT column) AS aggregated
  FROM
    onboarding_funnel_onboarding_completed
  GROUP BY
    funnel_id,
    action,
    element_type,
    submission_date,
    funnel
),
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    COALESCE(onboarding_funnel_first_card_aggregated.action) AS action,
    COALESCE(onboarding_funnel_first_card_aggregated.funnel_id) AS funnel_id,
    COALESCE(onboarding_funnel_first_card_aggregated.element_type) AS element_type,
    COALESCE(NULL) AS country,
    submission_date,
    funnel,
    COALESCE(onboarding_funnel_first_card_aggregated.aggregated) AS first_card,
    COALESCE(onboarding_funnel_second_card_aggregated.aggregated) AS second_card,
    COALESCE(onboarding_funnel_third_card_aggregated.aggregated) AS third_card,
    COALESCE(onboarding_funnel_onboarding_completed_aggregated.aggregated) AS onboarding_completed,
  FROM
    onboarding_funnel_first_card_aggregated
  FULL OUTER JOIN
    onboarding_funnel_second_card_aggregated
  USING
    (submission_date, action, funnel_id, element_type, country, funnel)
  FULL OUTER JOIN
    onboarding_funnel_third_card_aggregated
  USING
    (submission_date, action, funnel_id, element_type, country, funnel)
  FULL OUTER JOIN
    onboarding_funnel_onboarding_completed_aggregated
  USING
    (submission_date, action, funnel_id, element_type, country, funnel)
)
SELECT
  *
FROM
  merged_funnels
