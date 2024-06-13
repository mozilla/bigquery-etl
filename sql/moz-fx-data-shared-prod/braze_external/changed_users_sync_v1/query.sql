
-- Construct the JSON payload in Braze required format
SELECT
  CURRENT_TIMESTAMP() AS UPDATED_AT,
  external_id AS EXTERNAL_ID,
  TO_JSON(
    STRUCT(
      email AS email,
      email_subscribe AS email_subscribe,
      ARRAY_AGG(
        STRUCT(
          mailing_country AS mailing_country,
          basket_token AS basket_token,
          email_lang AS email_lang,
          has_fxa AS has_fxa,
          fxa_primary_email AS fxa_primary_email,
          fxa_lang AS fxa_lang,
          fxa_first_service AS fxa_first_service,
          CASE
            WHEN fxa_created_at IS NOT NULL
              THEN STRUCT(
                  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S UTC', fxa_created_at, 'UTC') AS `$time`
                )
            ELSE STRUCT(
                FORMAT_TIMESTAMP(
                  '%Y-%m-%d %H:%M:%E6S UTC',
                  '1900-01-01 00:00:00.000000 UTC',
                  'UTC'
                ) AS `$time`
              )
          END AS fxa_created_at,
          STRUCT(
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S UTC', create_timestamp, 'UTC') AS `$time`
          ) AS created_at,
          STRUCT(
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S UTC', update_timestamp, 'UTC') AS `$time`
          ) AS updated_at
        )
      ) AS user_attributes_v1
    )
  ) AS PAYLOAD
FROM
  `moz-fx-data-shared-prod.braze_external.changed_users_v1`
WHERE
  status IN ('Changed', 'New')
GROUP BY
  external_id,
  email,
  email_subscribe;
