SELECT
  CURRENT_TIMESTAMP() AS updated_at,
  submission_date,
  external_id,
  TO_JSON(
    STRUCT(
      email AS email,
      "subscribed" AS email_subscribe,
      [
        STRUCT(
          "718eea53-371c-4cc6-9fdc-1260b1311bd8" AS subscription_group_id,
          "subscribed" AS subscription_state
        )
      ] AS subscription_groups,
      locale AS locale,
      fxa_id_sha256
    )
  ) AS payload
FROM
  `moz-fx-data-shared-prod.braze_derived.fxa_win10_users_historical_v1`
WHERE
  submission_date = @submission_date
