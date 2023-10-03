WITH funnel1_signup AS (
  SELECT
    client_info.client_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS column
  FROM
    (
      SELECT
        *
      FROM
        mozdata.mozilla_vpn.events_unnested
      WHERE
        client_info.app_channel = 'production'
        AND client_info.os = 'iOS'
    )
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_name = 'authentication_inapp_step'
    AND `mozfun.map.get_key`(event_extra, 'state') = 'StateVerifyingSessionEmailCode'
),
funnel1_verify AS (
  SELECT
    client_info.client_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS column
  FROM
    (
      SELECT
        *
      FROM
        mozdata.mozilla_vpn.events_unnested
      WHERE
        client_info.app_channel = 'production'
        AND client_info.os = 'iOS'
    )
  INNER JOIN
    funnel1_signup AS prev
  ON
    prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_name = 'authentication_inapp_step'
    AND `mozfun.map.get_key`(event_extra, 'state') = 'StateVerifyingSessionEmailCode'
),
funnel1_start_subscription AS (
  SELECT
    client_info.client_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS column
  FROM
    (
      SELECT
        *
      FROM
        mozdata.mozilla_vpn.events_unnested
      WHERE
        client_info.app_channel = 'production'
        AND client_info.os = 'iOS'
    )
  INNER JOIN
    funnel1_verify AS prev
  ON
    prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_name = 'iap_subscription_started'
),
funnel2_signup AS (
  SELECT
    client_info.client_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS column
  FROM
    (
      SELECT
        *
      FROM
        mozdata.mozilla_vpn.events_unnested
      WHERE
        client_info.app_channel = 'production'
        AND client_info.os = 'iOS'
    )
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_name = 'authentication_inapp_step'
    AND `mozfun.map.get_key`(event_extra, 'state') = 'StateVerifyingSessionEmailCode'
),
funnel2_verify AS (
  SELECT
    client_info.client_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS column
  FROM
    (
      SELECT
        *
      FROM
        mozdata.mozilla_vpn.events_unnested
      WHERE
        client_info.app_channel = 'production'
        AND client_info.os = 'iOS'
    )
  INNER JOIN
    funnel2_signup AS prev
  ON
    prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_name = 'authentication_inapp_step'
    AND `mozfun.map.get_key`(event_extra, 'state') = 'StateVerifyingSessionEmailCode'
),
funnel2_error_subscription AS (
  SELECT
    client_info.client_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS column
  FROM
    (
      SELECT
        *
      FROM
        mozdata.mozilla_vpn.events_unnested
      WHERE
        client_info.app_channel = 'production'
        AND client_info.os = 'iOS'
    )
  INNER JOIN
    funnel2_verify AS prev
  ON
    prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_name = 'error_alert_shown'
),
funnel1_signup_aggregated AS (
  SELECT
    submission_date,
    "funnel1" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    funnel1_signup
  GROUP BY
    submission_date,
    funnel
),
funnel1_verify_aggregated AS (
  SELECT
    submission_date,
    "funnel1" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    funnel1_verify
  GROUP BY
    submission_date,
    funnel
),
funnel1_start_subscription_aggregated AS (
  SELECT
    submission_date,
    "funnel1" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    funnel1_start_subscription
  GROUP BY
    submission_date,
    funnel
),
funnel2_signup_aggregated AS (
  SELECT
    submission_date,
    "funnel2" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    funnel2_signup
  GROUP BY
    submission_date,
    funnel
),
funnel2_verify_aggregated AS (
  SELECT
    submission_date,
    "funnel2" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    funnel2_verify
  GROUP BY
    submission_date,
    funnel
),
funnel2_error_subscription_aggregated AS (
  SELECT
    submission_date,
    "funnel2" AS funnel,
    COUNT(column) AS aggregated
  FROM
    funnel2_error_subscription
  GROUP BY
    submission_date,
    funnel
),
merged_funnels AS (
  SELECT
    submission_date,
    funnel,
    COALESCE(funnel1_signup_aggregated.aggregated, funnel2_signup_aggregated.aggregated) AS signup,
    COALESCE(funnel1_verify_aggregated.aggregated, funnel2_verify_aggregated.aggregated) AS verify,
    COALESCE(funnel1_start_subscription_aggregated.aggregated, NULL) AS start_subscription,
    COALESCE(NULL, funnel2_error_subscription_aggregated.aggregated) AS error_subscription,
  FROM
    funnel1_signup_aggregated
  FULL OUTER JOIN
    funnel1_verify_aggregated
  USING
    (submission_date, funnel)
  FULL OUTER JOIN
    funnel1_start_subscription_aggregated
  USING
    (submission_date, funnel)
  FULL OUTER JOIN
    funnel2_signup_aggregated
  USING
    (submission_date, funnel)
  FULL OUTER JOIN
    funnel2_verify_aggregated
  USING
    (submission_date, funnel)
  FULL OUTER JOIN
    funnel2_error_subscription_aggregated
  USING
    (submission_date, funnel)
)
SELECT
  *
FROM
  merged_funnels
