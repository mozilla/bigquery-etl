WITH mozilla_space_emails AS (
  SELECT
    send.id AS send_id,
    TIMESTAMP_SECONDS(send.time) AS send_time,
    send.* EXCEPT (id, time, send_id),
  FROM
    `moz-fx-data-shared-prod.braze_external.braze_currents_mozilla_send_v1` AS send
  WHERE
    DATE(TIMESTAMP_SECONDS(send.time)) = @send_time
),
firefox_space_emails AS (
  SELECT
    send.id AS send_id,
    TIMESTAMP_SECONDS(send.time) AS send_time,
    send.* EXCEPT (id, time, send_id),
  FROM
    `moz-fx-data-shared-prod.braze_external.braze_currents_firefox_send_v1` AS send
  WHERE
    DATE(TIMESTAMP_SECONDS(send.time)) = @send_time
),
unioned AS (
  SELECT
    *
  FROM
    mozilla_space_emails
  UNION ALL
  SELECT
    *
  FROM
    firefox_space_emails
)
SELECT
  unioned.*,
  COALESCE(users.fxa_id_sha256, win10_users.fxa_id_sha256) AS fxa_id_sha256
FROM
  unioned
LEFT JOIN
  `moz-fx-data-shared-prod.braze_derived.users_v1` AS users
  ON users.external_id = unioned.external_user_id
LEFT JOIN
  `moz-fx-data-shared-prod.braze_derived.fxa_win10_users_historical_v1` AS win10_users
  ON win10_users.external_id = unioned.external_user_id
WHERE
  DATE(send_time) = @send_time
