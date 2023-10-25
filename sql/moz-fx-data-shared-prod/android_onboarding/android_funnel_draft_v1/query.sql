-- extract the relevant fields for each funnel step and segment if necessary
WITH onboarding_funnel_first_card AS (
  SELECT
    client_info.client_id AS join_key,
    mozfun.map.get_key(extra, 'sequence_id') AS funnel_id,
    onboarding_funnel_funnel_id.action AS action,
    onboarding_funnel_action.element_type AS element_type,
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
  LEFT JOIN
    (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id AS client_id,
        mozfun.map.get_key(extra, 'action') AS action
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
    ) AS dimension_source_action
  ON
    dimension_source_action.client_id = client_id
  LEFT JOIN
    (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id AS client_id,
        mozfun.map.get_key(extra, 'element_type') AS element_type
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
    ) AS dimension_source_element_type
  ON
    dimension_source_element_type.client_id = client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND mozfun.map.get_key(extra, 'sequence_position') = '1'
),
onboarding_funnel_second_card AS (
  SELECT
    client_info.client_id AS join_key,
    mozfun.map.get_key(extra, 'sequence_id') AS funnel_id,
    onboarding_funnel_funnel_id.action AS action,
    onboarding_funnel_action.element_type AS element_type,
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
    onboarding_funnel_first_card AS prev
  ON
    prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  LEFT JOIN
    (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id AS client_id,
        mozfun.map.get_key(extra, 'action') AS action
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
    ) AS dimension_source_action
  ON
    dimension_source_action.client_id = client_id
  LEFT JOIN
    (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id AS client_id,
        mozfun.map.get_key(extra, 'element_type') AS element_type
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
    ) AS dimension_source_element_type
  ON
    dimension_source_element_type.client_id = client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND mozfun.map.get_key(extra, 'sequence_position') = '2'
),
onboarding_funnel_third_card AS (
  SELECT
    client_info.client_id AS join_key,
    mozfun.map.get_key(extra, 'sequence_id') AS funnel_id,
    onboarding_funnel_funnel_id.action AS action,
    onboarding_funnel_action.element_type AS element_type,
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
    onboarding_funnel_second_card AS prev
  ON
    prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  LEFT JOIN
    (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id AS client_id,
        mozfun.map.get_key(extra, 'action') AS action
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
    ) AS dimension_source_action
  ON
    dimension_source_action.client_id = client_id
  LEFT JOIN
    (
      SELECT
        DATE(submission_timestamp) AS submission_date,
        client_info.client_id AS client_id,
        mozfun.map.get_key(extra, 'element_type') AS element_type
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
    ) AS dimension_source_element_type
  ON
    dimension_source_element_type.client_id = client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND mozfun.map.get_key(extra, 'sequence_position') = '3'
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
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    COALESCE(onboarding_funnel_first_card_aggregated.action) AS action,
    COALESCE(onboarding_funnel_first_card_aggregated.funnel_id) AS funnel_id,
    COALESCE(onboarding_funnel_first_card_aggregated.element_type) AS element_type,
    submission_date,
    funnel,
    COALESCE(onboarding_funnel_first_card_aggregated.aggregated) AS first_card,
    COALESCE(onboarding_funnel_second_card_aggregated.aggregated) AS second_card,
    COALESCE(onboarding_funnel_third_card_aggregated.aggregated) AS third_card,
  FROM
    onboarding_funnel_first_card_aggregated
  FULL OUTER JOIN
    onboarding_funnel_second_card_aggregated
  USING
    (submission_date, funnel)
  FULL OUTER JOIN
    onboarding_funnel_third_card_aggregated
  USING
    (submission_date, funnel)
)
SELECT
  *
FROM
  merged_funnels
