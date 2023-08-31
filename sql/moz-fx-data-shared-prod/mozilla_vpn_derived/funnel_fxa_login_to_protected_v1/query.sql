WITH fxa_login AS (
  SELECT
    -- One row per fxa_uid and one row per flow_id where fxa_uid is NULL
    COALESCE(fxa_uid, flow_id) AS id,
    MIN(flow_started) AS first_fxa_login,
    ANY_VALUE(fxa_uid) AS fxa_uid,
  FROM
    login_flows_v1
  LEFT JOIN
    UNNEST(fxa_uids) AS fxa_uid
  GROUP BY
    id
  HAVING
    -- Only include distinct users. Consider a flow_id without fxa_uid a distinct user if they
    -- viewed_email_first_page. This will over count incomplete login attempts where session
    -- information is not available from prior login attempts (e.g. switching browsers, expired
    -- sessions, etc).
    fxa_uid IS NOT NULL
    OR LOGICAL_OR(viewed_email_first_page)
),
completed_login AS (
  SELECT
    fxa_uid,
    MIN(flow_completed) AS first_completed_login,
  FROM
    login_flows_v1
  LEFT JOIN
    UNNEST(fxa_uids) AS fxa_uid
  WHERE
    fxa_uid IS NOT NULL
    AND flow_completed IS NOT NULL
  GROUP BY
    fxa_uid
),
registered_user AS (
  SELECT
    fxa_uid,
    MIN(created_at) AS first_registered_user
  FROM
    users_v1
  GROUP BY
    fxa_uid
),
paid_for_subscription AS (
  SELECT
    fxa_uid,
    MIN(customer_start_date) AS first_paid_for_subscription,
  FROM
    all_subscriptions_v1
  GROUP BY
    fxa_uid
),
registered_device AS (
  SELECT
    fxa_uid,
    MIN(`timestamp`) AS first_registered_device
  FROM
    add_device_events_v1
  GROUP BY
    fxa_uid
),
protected AS (
  SELECT
    fxa_uid,
    MIN(first_protected) AS first_protected
  FROM
    protected_v1
  GROUP BY
    fxa_uid
)
SELECT
  id,
  fxa_login.fxa_uid,
  DATE(first_fxa_login) AS start_date,
  (completed_login.fxa_uid IS NOT NULL) AS completed_login,
  (registered_user.fxa_uid IS NOT NULL) AS registered_user,
  (paid_for_subscription.fxa_uid IS NOT NULL) AS paid_for_subscription,
  (registered_device.fxa_uid IS NOT NULL) AS registered_device,
  (protected.fxa_uid IS NOT NULL) AS protected,
FROM
  fxa_login
LEFT JOIN
  completed_login
  ON first_completed_login
  BETWEEN first_fxa_login
  AND TIMESTAMP_ADD(first_fxa_login, INTERVAL 2 DAY)
  AND fxa_login.fxa_uid = completed_login.fxa_uid
LEFT JOIN
  registered_user
  ON first_registered_user
  BETWEEN first_fxa_login
  AND TIMESTAMP_ADD(first_fxa_login, INTERVAL 2 DAY)
  AND completed_login.fxa_uid = registered_user.fxa_uid
LEFT JOIN
  paid_for_subscription
  ON first_paid_for_subscription
  BETWEEN first_fxa_login
  AND TIMESTAMP_ADD(first_fxa_login, INTERVAL 2 DAY)
  AND registered_user.fxa_uid = paid_for_subscription.fxa_uid
LEFT JOIN
  registered_device
  ON first_registered_device
  BETWEEN first_fxa_login
  AND TIMESTAMP_ADD(first_fxa_login, INTERVAL 2 DAY)
  AND paid_for_subscription.fxa_uid = registered_device.fxa_uid
LEFT JOIN
  protected
  ON first_protected
  BETWEEN first_fxa_login
  AND TIMESTAMP_ADD(first_fxa_login, INTERVAL 2 DAY)
  AND registered_device.fxa_uid = protected.fxa_uid
WHERE
  DATE(first_fxa_login) >= '2020-05-01'
