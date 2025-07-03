SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  CONCAT('add-ons-devs-', SUBSTR(TO_HEX(SHA256(GENERATE_UUID())), 1, 23)) AS EXTERNAL_ID,
  TO_JSON(
    STRUCT(
      email AS email,
      "subscribed" AS email_subscribe,
      ARRAY_AGG(
        STRUCT(
          "a0159f46-c559-4922-89a0-61ed2c8e0e4f" AS subscription_group_id,
          "subscribed" AS subscription_state
        )
      ) AS subscription_groups
    )
  ) AS PAYLOAD
FROM
  `moz-fx-data-shared-prod.braze_derived.add_ons_developers_v1`
GROUP BY
  email
