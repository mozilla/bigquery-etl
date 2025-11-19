WITH mozilla_space_emails AS (
  SELECT
    open.id AS open_id,
    TIMESTAMP_SECONDS(open.time) AS open_time,
    open.* EXCEPT (id, time),
  FROM
    `moz-fx-data-shared-prod.braze_external.braze_currents_mozilla_open_v1` AS open
  WHERE
    DATE(TIMESTAMP_SECONDS(open.time)) = @open_time
),
firefox_space_emails AS (
  SELECT
    open.id AS open_id,
    TIMESTAMP_SECONDS(open.time) AS open_time,
    open.* EXCEPT (id, time),
  FROM
    `moz-fx-data-shared-prod.braze_external.braze_currents_firefox_open_v1` AS open
  WHERE
    DATE(TIMESTAMP_SECONDS(open.time)) = @open_time
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
  DATE(open_time) = @open_time
