-- extract the relevant fields for each funnel step and segment if necessary
WITH onboarding_funnel_first_card_impression AS (
  SELECT
    client_info.client_id AS join_key,
    mozfun.map.get_key(extra, 'sequence_id') AS funnel_id,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    (
      SELECT
        *
      FROM
        mozdata.fenix.events ev
      CROSS JOIN
        UNNEST(events) e
      WHERE
        e.category = 'onboarding'
    )
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND mozfun.map.get_key(extra, 'sequence_position') = '1'
    AND mozfun.map.get_key(extra, 'action') = 'impression'
),
onboarding_funnel_first_card_primary_click AS (
  SELECT
    client_info.client_id AS join_key,
    mozfun.map.get_key(extra, 'sequence_id') AS funnel_id,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    (
      SELECT
        *
      FROM
        mozdata.fenix.events ev
      CROSS JOIN
        UNNEST(events) e
      WHERE
        e.category = 'onboarding'
    )
  INNER JOIN
    onboarding_funnel_first_card_impression AS prev
  ON
    prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND mozfun.map.get_key(extra, 'sequence_position') = '1'
    AND mozfun.map.get_key(extra, 'action') = 'click'
    AND mozfun.map.get_key(extra, 'element_type') = 'primary_button'
),
onboarding_funnel_second_card_impression AS (
  SELECT
    client_info.client_id AS join_key,
    mozfun.map.get_key(extra, 'sequence_id') AS funnel_id,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    (
      SELECT
        *
      FROM
        mozdata.fenix.events ev
      CROSS JOIN
        UNNEST(events) e
      WHERE
        e.category = 'onboarding'
    )
  INNER JOIN
    onboarding_funnel_first_card_primary_click AS prev
  ON
    prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND mozfun.map.get_key(extra, 'sequence_position') = '2'
    AND mozfun.map.get_key(extra, 'action') = 'impression'
),
onboarding_funnel_second_card_primary_click AS (
  SELECT
    client_info.client_id AS join_key,
    mozfun.map.get_key(extra, 'sequence_id') AS funnel_id,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    (
      SELECT
        *
      FROM
        mozdata.fenix.events ev
      CROSS JOIN
        UNNEST(events) e
      WHERE
        e.category = 'onboarding'
    )
  INNER JOIN
    onboarding_funnel_second_card_impression AS prev
  ON
    prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND mozfun.map.get_key(extra, 'sequence_position') = '2'
    AND mozfun.map.get_key(extra, 'action') = 'click'
    AND mozfun.map.get_key(extra, 'element_type') = 'primary_button'
),
onboarding_funnel_third_card_impression AS (
  SELECT
    client_info.client_id AS join_key,
    mozfun.map.get_key(extra, 'sequence_id') AS funnel_id,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    (
      SELECT
        *
      FROM
        mozdata.fenix.events ev
      CROSS JOIN
        UNNEST(events) e
      WHERE
        e.category = 'onboarding'
    )
  INNER JOIN
    onboarding_funnel_second_card_primary_click AS prev
  ON
    prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND mozfun.map.get_key(extra, 'sequence_position') = '3'
    AND mozfun.map.get_key(extra, 'action') = 'impression'
),
onboarding_funnel_third_card_primary_click AS (
  SELECT
    client_info.client_id AS join_key,
    mozfun.map.get_key(extra, 'sequence_id') AS funnel_id,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    client_info.client_id AS column
  FROM
    (
      SELECT
        *
      FROM
        mozdata.fenix.events ev
      CROSS JOIN
        UNNEST(events) e
      WHERE
        e.category = 'onboarding'
    )
  INNER JOIN
    onboarding_funnel_third_card_impression AS prev
  ON
    prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND mozfun.map.get_key(extra, 'sequence_position') = '3'
    AND mozfun.map.get_key(extra, 'action') = 'click'
    AND mozfun.map.get_key(extra, 'element_type') = 'primary_button'
),
-- aggregate each funnel step value
onboarding_funnel_first_card_impression_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
    COUNT(DISTINCT column) AS aggregated
  FROM
    onboarding_funnel_first_card_impression
  GROUP BY
    funnel_id,
    submission_date,
    funnel
),
onboarding_funnel_first_card_primary_click_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
    COUNT(DISTINCT column) AS aggregated
  FROM
    onboarding_funnel_first_card_primary_click
  GROUP BY
    funnel_id,
    submission_date,
    funnel
),
onboarding_funnel_second_card_impression_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
    COUNT(DISTINCT column) AS aggregated
  FROM
    onboarding_funnel_second_card_impression
  GROUP BY
    funnel_id,
    submission_date,
    funnel
),
onboarding_funnel_second_card_primary_click_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
    COUNT(DISTINCT column) AS aggregated
  FROM
    onboarding_funnel_second_card_primary_click
  GROUP BY
    funnel_id,
    submission_date,
    funnel
),
onboarding_funnel_third_card_impression_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
    COUNT(DISTINCT column) AS aggregated
  FROM
    onboarding_funnel_third_card_impression
  GROUP BY
    funnel_id,
    submission_date,
    funnel
),
onboarding_funnel_third_card_primary_click_aggregated AS (
  SELECT
    submission_date,
    "onboarding_funnel" AS funnel,
    funnel_id,
    COUNT(DISTINCT column) AS aggregated
  FROM
    onboarding_funnel_third_card_primary_click
  GROUP BY
    funnel_id,
    submission_date,
    funnel
),
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    COALESCE(NULL) AS action,
    COALESCE(onboarding_funnel_first_card_impression_aggregated.funnel_id) AS funnel_id,
    COALESCE(NULL) AS element_type,
    submission_date,
    funnel,
    COALESCE(
      onboarding_funnel_first_card_impression_aggregated.aggregated
    ) AS first_card_impression,
    COALESCE(
      onboarding_funnel_first_card_primary_click_aggregated.aggregated
    ) AS first_card_primary_click,
    COALESCE(
      onboarding_funnel_second_card_impression_aggregated.aggregated
    ) AS second_card_impression,
    COALESCE(
      onboarding_funnel_second_card_primary_click_aggregated.aggregated
    ) AS second_card_primary_click,
    COALESCE(
      onboarding_funnel_third_card_impression_aggregated.aggregated
    ) AS third_card_impression,
    COALESCE(
      onboarding_funnel_third_card_primary_click_aggregated.aggregated
    ) AS third_card_primary_click,
  FROM
    onboarding_funnel_first_card_impression_aggregated
  FULL OUTER JOIN
    onboarding_funnel_first_card_primary_click_aggregated
  USING
    (submission_date, funnel)
  FULL OUTER JOIN
    onboarding_funnel_second_card_impression_aggregated
  USING
    (submission_date, funnel)
  FULL OUTER JOIN
    onboarding_funnel_second_card_primary_click_aggregated
  USING
    (submission_date, funnel)
  FULL OUTER JOIN
    onboarding_funnel_third_card_impression_aggregated
  USING
    (submission_date, funnel)
  FULL OUTER JOIN
    onboarding_funnel_third_card_primary_click_aggregated
  USING
    (submission_date, funnel)
)
SELECT
  *
FROM
  merged_funnels
