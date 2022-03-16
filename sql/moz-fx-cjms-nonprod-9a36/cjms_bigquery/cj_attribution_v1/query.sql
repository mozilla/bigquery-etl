WITH nonprod_fxa_content_auth_stdout_events_live AS (
  -- for reduced latency in stage, inline live version of
  -- mozdata.firefox_accounts.nonprod_fxa_content_auth_stdout_events
  WITH nonprod_fxa_auth_events_live AS (
    SELECT
      * REPLACE (
        (
          SELECT AS STRUCT
            jsonPayload.* REPLACE (
              (
                SELECT AS STRUCT
                  jsonPayload.fields.* EXCEPT (device_id, user_id) REPLACE(
                    -- See https://bugzilla.mozilla.org/show_bug.cgi?id=1707571
                    CAST(NULL AS FLOAT64) AS emailverified,
                    CAST(NULL AS FLOAT64) AS isprimary,
                    CAST(NULL AS FLOAT64) AS isverified
                  ),
                  TO_HEX(SHA256(jsonPayload.fields.user_id)) AS user_id
              ) AS fields
            )
        ) AS jsonPayload
      )
    FROM
      `moz-fx-fxa-nonprod-375e.fxa_stage_logs.docker_fxa_auth_20*`
    WHERE
      jsonPayload.type = 'amplitudeEvent'
      AND jsonPayload.fields.event_type IS NOT NULL
      AND jsonPayload.fields.user_id IS NOT NULL
      AND _TABLE_SUFFIX >= FORMAT_DATE('%y%m%d', CURRENT_DATE - 1)
  ),
  nonprod_fxa_content_events_live AS (
    SELECT
      * REPLACE (
        (
          SELECT AS STRUCT
            jsonPayload.* REPLACE (
              (
                SELECT AS STRUCT
                  jsonPayload.fields.* EXCEPT (device_id, user_id),
                  TO_HEX(SHA256(jsonPayload.fields.user_id)) AS user_id
              ) AS fields
            )
        ) AS jsonPayload
      )
    FROM
      `moz-fx-fxa-nonprod-375e.fxa_stage_logs.docker_fxa_content_20*`
    WHERE
      jsonPayload.type = 'amplitudeEvent'
      AND jsonPayload.fields.event_type IS NOT NULL
      AND _TABLE_SUFFIX >= FORMAT_DATE('%y%m%d', CURRENT_DATE - 1)
  ),
  nonprod_fxa_stdout_events_live AS (
    SELECT
      * REPLACE (
        (
          SELECT AS STRUCT
            jsonPayload.* REPLACE (
              (
                SELECT AS STRUCT
                  jsonPayload.fields.* EXCEPT (device_id, user_id),
                  TO_HEX(SHA256(jsonPayload.fields.user_id)) AS user_id
              ) AS fields
            )
        ) AS jsonPayload
      )
    FROM
      `moz-fx-fxa-nonprod-375e.fxa_stage_logs.stdout_20*`
    WHERE
      jsonPayload.type = 'amplitudeEvent'
      AND jsonPayload.fields.event_type IS NOT NULL
      AND _TABLE_SUFFIX >= FORMAT_DATE('%y%m%d', CURRENT_DATE - 1)
  ),
  content AS (
    SELECT
      jsonPayload.logger,
      jsonPayload.fields.event_type,
      jsonPayload.fields.app_version,
      jsonPayload.fields.os_name,
      jsonPayload.fields.os_version,
      jsonPayload.fields.country,
      jsonPayload.fields.language,
      jsonPayload.fields.user_id,
      jsonPayload.fields.user_properties,
      jsonPayload.fields.event_properties,
      `timestamp`,
      receiveTimestamp
    FROM
      nonprod_fxa_content_events_live
  ),
  --
  auth AS (
    SELECT
      jsonPayload.logger,
      jsonPayload.fields.event_type,
      jsonPayload.fields.app_version,
      jsonPayload.fields.os_name,
      jsonPayload.fields.os_version,
      jsonPayload.fields.country,
      jsonPayload.fields.language,
      jsonPayload.fields.user_id,
      jsonPayload.fields.user_properties,
      jsonPayload.fields.event_properties,
      `timestamp`,
      receiveTimestamp
    FROM
      nonprod_fxa_auth_events_live
  ),
  --
  stdout AS (
    SELECT
      jsonPayload.logger,
      jsonPayload.fields.event_type,
      jsonPayload.fields.app_version,
      jsonPayload.fields.os_name,
      jsonPayload.fields.os_version,
      CAST(NULL AS STRING) AS country,
      CAST(NULL AS STRING) AS language,
      jsonPayload.fields.user_id,
      jsonPayload.fields.user_properties,
      jsonPayload.fields.event_properties,
      `timestamp`,
      receiveTimestamp
    FROM
      nonprod_fxa_stdout_events_live
  ),
  --
  unioned AS (
    SELECT
      *
    FROM
      auth
    UNION ALL
    SELECT
      *
    FROM
      content
    UNION ALL
    SELECT
      *
    FROM
      stdout
  )
  --
  SELECT
    * EXCEPT (user_properties, event_properties),
    REPLACE(JSON_EXTRACT(user_properties, '$.utm_term'), "\"", "") AS utm_term,
    REPLACE(JSON_EXTRACT(user_properties, '$.utm_source'), "\"", "") AS utm_source,
    REPLACE(JSON_EXTRACT(user_properties, '$.utm_medium'), "\"", "") AS utm_medium,
    REPLACE(JSON_EXTRACT(user_properties, '$.utm_campaign'), "\"", "") AS utm_campaign,
    REPLACE(JSON_EXTRACT(user_properties, '$.utm_content'), "\"", "") AS utm_content,
    REPLACE(JSON_EXTRACT(user_properties, '$.ua_version'), "\"", "") AS ua_version,
    REPLACE(JSON_EXTRACT(user_properties, '$.ua_browser'), "\"", "") AS ua_browser,
    REPLACE(JSON_EXTRACT(user_properties, '$.entrypoint'), "\"", "") AS entrypoint,
    REPLACE(
      JSON_EXTRACT(user_properties, '$.entrypoint_experiment'),
      "\"",
      ""
    ) AS entrypoint_experiment,
    REPLACE(
      JSON_EXTRACT(user_properties, '$.entrypoint_variation'),
      "\"",
      ""
    ) AS entrypoint_variation,
    REPLACE(JSON_EXTRACT(user_properties, '$.flow_id'), "\"", "") AS flow_id,
    REPLACE(JSON_EXTRACT(event_properties, '$.service'), "\"", "") AS service,
    REPLACE(JSON_EXTRACT(event_properties, '$.email_type'), "\"", "") AS email_type,
    REPLACE(JSON_EXTRACT(event_properties, '$.email_provider'), "\"", "") AS email_provider,
    REPLACE(JSON_EXTRACT(event_properties, '$.oauth_client_id'), "\"", "") AS oauth_client_id,
    REPLACE(
      JSON_EXTRACT(event_properties, '$.connect_device_flow'),
      "\"",
      ""
    ) AS connect_device_flow,
    REPLACE(JSON_EXTRACT(event_properties, '$.connect_device_os'), "\"", "") AS connect_device_os,
    REPLACE(JSON_EXTRACT(user_properties, '$.sync_device_count'), "\"", "") AS sync_device_count,
    REPLACE(
      JSON_EXTRACT(user_properties, '$.sync_active_devices_day'),
      "\"",
      ""
    ) AS sync_active_devices_day,
    REPLACE(
      JSON_EXTRACT(user_properties, '$.sync_active_devices_week'),
      "\"",
      ""
    ) AS sync_active_devices_week,
    REPLACE(
      JSON_EXTRACT(user_properties, '$.sync_active_devices_month'),
      "\"",
      ""
    ) AS sync_active_devices_month,
    REPLACE(JSON_EXTRACT(event_properties, '$.email_sender'), "\"", "") AS email_sender,
    REPLACE(JSON_EXTRACT(event_properties, '$.email_service'), "\"", "") AS email_service,
    REPLACE(JSON_EXTRACT(event_properties, '$.email_template'), "\"", "") AS email_template,
    REPLACE(JSON_EXTRACT(event_properties, '$.email_version'), "\"", "") AS email_version,
    REPLACE(JSON_EXTRACT(event_properties, '$.plan_id'), "\"", "") AS plan_id,
    REPLACE(JSON_EXTRACT(event_properties, '$.product_id'), "\"", "") AS product_id,
    REPLACE(JSON_EXTRACT(event_properties, '$.promotionCode'), "\"", "") AS promotion_code,
  FROM
    unioned
  UNION ALL
  SELECT
    *
  FROM
    mozdata.firefox_accounts.nonprod_fxa_content_auth_stdout_events
  WHERE
    -- only select dates that aren't being pulled live
    DATE(`timestamp`) < CURRENT_DATE - 1
),
daily_flows AS (
  SELECT
    DATE(`timestamp`) AS submission_date,
    flow_id,
    MIN(`timestamp`) AS flow_started,
    ARRAY_AGG(
      IF(
        user_id IS NULL,
        NULL,
        STRUCT(user_id AS fxa_uid, `timestamp` AS fxa_uid_timestamp)
      ) IGNORE NULLS
      ORDER BY
        `timestamp` DESC
      LIMIT
        1
    )[SAFE_OFFSET(0)].*,
  FROM
    nonprod_fxa_content_auth_stdout_events_live
  WHERE
    DATE(`timestamp`) >= CURRENT_DATE - 30
    -- IF(
    --   SAFE_CAST(@submission_date AS DATE) IS NULL,
    --   DATE(`timestamp`) >= CURRENT_DATE - 30,
    --   DATE(`timestamp`) = @submission_date
    -- )
    AND flow_id IS NOT NULL
  GROUP BY
    submission_date,
    flow_id
),
flows AS (
  -- last fxa_uid for each flow_id
  SELECT
    flow_id,
    MIN(flow_started) AS flow_started,
    ARRAY_AGG(fxa_uid IGNORE NULLS ORDER BY fxa_uid_timestamp DESC LIMIT 1)[
      SAFE_OFFSET(0)
    ] AS fxa_uid,
  FROM
    daily_flows
  GROUP BY
    flow_id
),
attributed_flows AS (
  -- last flow_id in aic for each fxa_uid
  SELECT
    fxa_uid,
    ARRAY_AGG(STRUCT(flow_id, flow_started) ORDER BY flow_started DESC LIMIT 1)[SAFE_OFFSET(0)].*,
  FROM
    flows
  JOIN
    EXTERNAL_QUERY("moz-fx-cjms-nonprod-9a36.us.cjms-sql", "SELECT flow_id FROM aic")
  USING
    (flow_id)
  GROUP BY
    fxa_uid
),
nonprod_stripe_subscriptions AS (
  -- For reduced latency in stage inline fivetran version of stripe subscriptions
  WITH subscription AS (
    SELECT
      customer_id,
      id AS subscription_id,
      status,
      _fivetran_synced,
      COALESCE(trial_end, start_date) AS subscription_start_date,
      created,
      -- trial_start,
      trial_end,
      -- current_period_end,
      -- current_period_start,
      canceled_at,
      JSON_VALUE(metadata, "$.cancelled_for_customer_at") AS canceled_for_customer_at,
      cancel_at,
      cancel_at_period_end,
      IF(ended_at <= _fivetran_synced, ended_at, NULL) AS ended_at,
    FROM
      `dev-fivetran`.stripe_nonprod.subscription_history
    WHERE
      status NOT IN ("incomplete", "incomplete_expired")
      AND _fivetran_active
  ),
  subscription_item AS (
    SELECT
      id AS subscription_item_id,
      subscription_id,
      plan_id AS plan_id,
    FROM
      `dev-fivetran`.stripe_nonprod.subscription_item
  ),
  customer AS (
    SELECT
      id AS customer_id,
      TO_HEX(SHA256(JSON_VALUE(metadata, "$.userid"))) AS fxa_uid,
      address_country,
    FROM
      `dev-fivetran`.stripe_nonprod.customer
  ),
  charge AS (
    SELECT
      charge.id AS charge_id,
      COALESCE(card.country, charge.billing_detail_address_country) AS country,
    FROM
      `dev-fivetran`.stripe_nonprod.charge
    JOIN
      `dev-fivetran`.stripe_nonprod.card
    ON
      charge.card_id = card.id
    WHERE
      charge.status = "succeeded"
  ),
  invoice_provider_country AS (
    SELECT
      invoice_line_item.subscription_id,
      IF(
        JSON_VALUE(invoice.metadata, "$.paypalTransactionId") IS NOT NULL,
        -- FxA copies paypal billing agreement country to customer address
        STRUCT("Paypal" AS provider, customer.address_country AS country),
        ("Stripe", charge.country)
      ).*,
      invoice.finalized_at,
    FROM
      `dev-fivetran`.stripe_nonprod.invoice
    LEFT JOIN
      `dev-fivetran`.stripe_nonprod.invoice_line_item
    ON
      invoice.id = invoice_line_item.invoice_id
    LEFT JOIN
      customer
    USING
      (customer_id)
    LEFT JOIN
      charge
    USING
      (charge_id)
  ),
  subscription_promotion_codes AS (
    SELECT
      subscription_discount.subscription_id,
      ARRAY_AGG(DISTINCT promotion_code.code IGNORE NULLS) AS promotion_codes,
    FROM
      `dev-fivetran`.stripe_nonprod.promotion_code
    JOIN
      `dev-fivetran`.stripe_nonprod.subscription_discount
    USING
      (coupon_id, customer_id)
    GROUP BY
      subscription_id
  ),
  subscription_provider_country AS (
    SELECT
      subscription_id,
      ARRAY_AGG(
        STRUCT(provider, LOWER(country) AS country)
        ORDER BY
          -- prefer rows with country
          IF(country IS NULL, 0, 1) DESC,
          finalized_at DESC
        LIMIT
          1
      )[OFFSET(0)].*
    FROM
      invoice_provider_country
    GROUP BY
      subscription_id
  ),
  plan AS (
    SELECT
      plan.id AS plan_id,
      plan.amount AS plan_amount,
      plan.billing_scheme AS billing_scheme,
      plan.currency AS plan_currency,
      plan.interval AS plan_interval,
      plan.interval_count AS plan_interval_count,
      plan.product_id,
      product.name AS product_name,
    FROM
      `dev-fivetran`.stripe_nonprod.plan
    LEFT JOIN
      `dev-fivetran`.stripe_nonprod.product
    ON
      plan.product_id = product.id
  )
  SELECT
    customer_id,
    subscription_id,
    subscription_item_id,
    plan_id,
    status,
    _fivetran_synced AS event_timestamp,
    MIN(subscription_start_date) OVER (PARTITION BY customer_id) AS customer_start_date,
    subscription_start_date,
    created,
    trial_end,
    canceled_at,
    canceled_for_customer_at,
    cancel_at,
    cancel_at_period_end,
    ended_at,
    fxa_uid,
    country,
    provider,
    plan_amount,
    billing_scheme,
    plan_currency,
    plan_interval,
    plan_interval_count,
    "Etc/UTC" AS plan_interval_timezone,
    product_id,
    product_name,
    promotion_codes,
  FROM
    subscription
  LEFT JOIN
    subscription_item
  USING
    (subscription_id)
  LEFT JOIN
    plan
  USING
    (plan_id)
  LEFT JOIN
    subscription_provider_country
  USING
    (subscription_id)
  LEFT JOIN
    customer
  USING
    (customer_id)
  LEFT JOIN
    subscription_promotion_codes
  USING
    (subscription_id)
),
attributed_subs AS (
  -- last subscription_id created after the flow started for each fxa_uid
  SELECT
    attributed_flows.fxa_uid,
    ARRAY_AGG(
      nonprod_stripe_subscriptions.subscription_id
      ORDER BY
        nonprod_stripe_subscriptions.created DESC
      LIMIT
        1
    )[SAFE_OFFSET(0)] AS subscription_id,
  FROM
    attributed_flows
  JOIN
    --mozdata.subscription_platform.nonprod_stripe_subscriptions
    nonprod_stripe_subscriptions
  ON
    attributed_flows.fxa_uid = nonprod_stripe_subscriptions.fxa_uid
    AND attributed_flows.flow_started < nonprod_stripe_subscriptions.created
  GROUP BY
    fxa_uid
)
SELECT
  CURRENT_TIMESTAMP AS report_timestamp,
  created AS subscription_created,
  subscription_id, -- transaction id
  fxa_uid,
  1 AS quantity,
  plan_id, -- sku
  plan_currency,
  plan_amount,
  country,
  flow_id,
FROM
  attributed_subs
JOIN
  attributed_flows
USING
  (fxa_uid)
JOIN
  --mozdata.subscription_platform.nonprod_stripe_subscriptions
  nonprod_stripe_subscriptions
USING
  (fxa_uid, subscription_id)
