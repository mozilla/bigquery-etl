SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  changed_users.external_id AS EXTERNAL_ID,
  TO_JSON(
    STRUCT(
      ARRAY_AGG(
        STRUCT(
          changed_users.email AS email,
          changed_users.mailing_country AS mailing_country,
          changed_users.email_subscribe AS email_subscribe,
          changed_users.basket_token AS basket_token,
          changed_users.email_lang AS email_lang,
          changed_users.has_fxa AS has_fxa,
          changed_users.fxa_primary_email AS fxa_primary_email,
          changed_users.fxa_lang AS fxa_lang,
          changed_users.fxa_first_service AS fxa_first_service,
          changed_users.fxa_created_date AS fxa_created_date,
          -- braze required format for nested timestamps
          STRUCT(
            FORMAT_TIMESTAMP(
              '%Y-%m-%d %H:%M:%E6S UTC',
              changed_users.create_timestamp,
              'UTC'
            ) AS `$time`
          ) AS create_timestamp,
          STRUCT(
            FORMAT_TIMESTAMP(
              '%Y-%m-%d %H:%M:%E6S UTC',
              changed_users.update_timestamp,
              'UTC'
            ) AS `$time`
          ) AS update_timestamp
        )
        ORDER BY
          changed_users.update_timestamp DESC
      ) AS user_attributes
    )
  ) AS PAYLOAD
FROM
  `moz-fx-data-shared-prod.braze_external.changed_users_v1` AS changed_users
WHERE
  changed_users.status = 'Changed'
GROUP BY
  changed_users.external_id;
