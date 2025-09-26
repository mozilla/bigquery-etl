SELECT
  CURRENT_TIMESTAMP() AS updated_at,
  IFNULL(external_id, TO_HEX(SHA256(GENERATE_UUID()))) AS external_id,
  TO_JSON(
    STRUCT(
      email AS email,
      "subscribed" AS email_subscribe,
      ARRAY_AGG(
        STRUCT("TODO" AS subscription_group_id, "subscribed" AS subscription_state)
      ) AS subscription_groups,
      locale AS locale,
      user_id_sha256 AS fxa_id_sha256
    )
  ) AS payload,
  user_id_sha256 AS fxa_id_sha256
FROM
  `moz-fx-data-shared-prod.braze_derived.fxa_win10_users_v1`
