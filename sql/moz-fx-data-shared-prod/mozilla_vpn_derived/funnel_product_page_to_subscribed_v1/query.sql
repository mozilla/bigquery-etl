WITH pricing_plans AS (
  SELECT
    id AS plan_id,
    product AS product_id,
    mozfun.vpn.pricing_plan(
      provider => "Stripe",
      amount => amount,
      currency => currency,
      `interval` => `interval`,
      interval_count => interval_count
    ) AS pricing_plan,
    nickname AS plan_name,
  FROM
    mozdata.stripe.plans
),
events AS (
  SELECT
    *,
    DATE(`timestamp`) AS partition_date,
  FROM
    mozdata.firefox_accounts.fxa_content_auth_stdout_events
),
flows AS (
  SELECT
    partition_date,
    flow_id,
    ARRAY_AGG(country IGNORE NULLS ORDER BY `timestamp` LIMIT 1)[SAFE_OFFSET(0)] AS country,
    ARRAY_AGG(
      IF(
        entrypoint IS NOT NULL
        OR entrypoint_experiment IS NOT NULL
        OR entrypoint_variation IS NOT NULL
        OR utm_campaign IS NOT NULL
        OR utm_content IS NOT NULL
        OR utm_medium IS NOT NULL
        OR utm_source IS NOT NULL
        OR utm_term IS NOT NULL,
        STRUCT(
          entrypoint,
          entrypoint_experiment,
          entrypoint_variation,
          utm_campaign,
          utm_content,
          utm_medium,
          utm_source,
          utm_term
        ),
        NULL
      ) IGNORE NULLS
      ORDER BY
        `timestamp`
      LIMIT
        1
    )[SAFE_OFFSET(0)].*,
    ARRAY_AGG(
      IF(
        ua_browser IS NOT NULL
        OR ua_version IS NOT NULL
        OR os_name IS NOT NULL
        OR os_version IS NOT NULL,
        STRUCT(ua_browser, ua_version, os_name, os_version),
        NULL
      ) IGNORE NULLS
      ORDER BY
        `timestamp`
      LIMIT
        1
    )[SAFE_OFFSET(0)].*,
    ARRAY_AGG(plan_id IGNORE NULLS ORDER BY `timestamp` LIMIT 1)[SAFE_OFFSET(0)] AS plan_id,
    LOGICAL_OR(event_type = "fxa_rp_button - view") AS rp_button_view,
    -- impression for the cta button
    LOGICAL_OR(event_type = "fxa_pay_account_setup - view") AS pay_account_setup_view,
    -- impression for entering email
    LOGICAL_OR(event_type = "fxa_pay_account_setup - engage") AS pay_account_setup_engage,
    -- either new fxa or existing fxa signed off
    LOGICAL_OR(event_type = "fxa_pay_account_setup - other") AS pay_account_setup_other,
    --sign in cta click
    LOGICAL_OR(event_type = "fxa_pay_setup - view") AS pay_setup_view,
    -- impression for payment set up
    LOGICAL_OR(
      event_type = "fxa_pay_setup - view"
      AND user_id IS NULL
    ) AS pay_setup_view_without_uid,
    LOGICAL_OR(
      event_type = "fxa_pay_setup - view"
      AND user_id IS NOT NULL
    ) AS pay_setup_view_with_uid,
    -- existing fxa after sign in
    LOGICAL_OR(event_type = "fxa_pay_setup - engage") AS pay_setup_engage,
    LOGICAL_OR(
      event_type = "fxa_pay_setup - engage"
      AND user_id IS NULL
    ) AS pay_setup_engage_without_uid,
    LOGICAL_OR(
      event_type = "fxa_pay_setup - engage"
      AND user_id IS NOT NULL
    ) AS pay_setup_engage_with_uid,
    -- new fxa after entering the email
    LOGICAL_OR(event_type = "fxa_pay_setup - 3ds_complete") AS pay_setup_complete,
    LOGICAL_OR(
      event_type = "fxa_pay_setup - 3ds_complete"
      AND user_id IS NULL
    ) AS pay_setup_complete_without_uid,
    LOGICAL_OR(
      event_type = "fxa_pay_setup - 3ds_complete"
      AND user_id IS NOT NULL
    ) AS pay_setup_complete_with_uid,
  FROM
    events
  WHERE
    IF(
      @date IS NULL,
      -- The fully FxA-based funnel for VPN was enabled on 2021-08-25.
      (partition_date >= "2021-08-25" AND partition_date < CURRENT_DATE),
      partition_date = @date
    )
    AND flow_id IS NOT NULL
  GROUP BY
    partition_date,
    flow_id
  HAVING
    -- NOTE: flows near date boundaries may not meet this condition for all dates
    LOGICAL_OR(
      service = "guardian-vpn"
      OR events.entrypoint = "www.mozilla.org-vpn-product-page"
      OR events.utm_source = "www.mozilla.org-vpn-product-page"
      OR (
        event_type = "fxa_rp_button - view"
        AND (
          -- The www.mozilla.org navbar CTA button was changed to link to VPN for Firefox users
          -- on 2021-09-02, then attribution was implemented for it on 2021-09-15.
          (
            service IS NULL
            AND events.utm_source = "www.mozilla.org"
            AND events.utm_campaign = "navigation"
            AND (partition_date BETWEEN "2021-09-15" AND "2021-12-08")
          )
          -- Service attribution was implemented for VPN FxA links on www.mozilla.org on 2021-12-08,
          -- but the service ended up as "undefined_oauth" until it was fixed on 2022-01-06.
          OR (
            service = "undefined_oauth"
            AND (partition_date BETWEEN "2021-12-08" AND "2022-01-06")
          )
        )
      )
      -- Even when service attribution for VPN FxA links was fixed on 2022-01-06 there was still a
      -- problem with service attribution for FxA payment events, which was fixed on 2022-03-09.
      OR (
        event_type LIKE r"fxa\_pay\_%"
        AND service = "undefined_oauth"
        AND (partition_date BETWEEN "2021-12-08" AND "2022-03-09")
      )
    )
),
flow_counts AS (
  SELECT
    partition_date,
    country,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    utm_term,
    entrypoint_experiment,
    entrypoint_variation,
    ua_browser,
    ua_version,
    os_name,
    os_version,
    entrypoint,
    plan_id,
    COUNTIF(rp_button_view) AS rp_button_view,
    -- vpn product site hits
    COUNTIF(pay_setup_view) AS pay_setup_view,
    -- mix of new fxa and existing fxa logged off
    COUNTIF(pay_setup_view_without_uid) AS pay_setup_view_without_uid,
    -- mix of new fxa and existing fxa logged off
    COUNTIF(pay_account_setup_view) AS pay_account_setup_view,
    COUNTIF(pay_account_setup_engage) AS pay_account_setup_engage,
    COUNTIF(pay_account_setup_engage AND pay_account_setup_other) AS pay_account_setup_engage_other,
    -- when existing fxa users enter their email then click the cta
    COUNTIF(pay_account_setup_other) AS pay_account_setup_other,
    COUNTIF(pay_setup_view_with_uid) AS pay_setup_view_with_uid,
    -- only existing fxa after log in
    COUNTIF(
      pay_setup_view_with_uid
      AND NOT pay_account_setup_other
    ) AS existing_fxa_signedin_pay_setup_view,
    -- logged in existing fxa user comes here as the 1st event
    COUNTIF(pay_setup_engage_without_uid) AS pay_setup_engage_without_uid,
    COUNTIF(pay_setup_engage_with_uid) AS pay_setup_engage_with_uid,
    COUNTIF(
      pay_setup_engage_with_uid
      AND pay_account_setup_other
    ) AS existing_fxa_signedoff_pay_setup_engage,
    -- COUNTIF(pay_account_setup_engage and pay_setup_engage_without_uid) AS new_fxa_pay_setup_engage,
    COUNTIF(pay_setup_complete) AS pay_setup_complete,
    COUNTIF(
      pay_setup_complete_without_uid
      AND pay_setup_engage_without_uid
    ) AS pay_setup_complete_without_uid,
    COUNTIF(
      pay_setup_complete_with_uid
      AND pay_setup_engage_with_uid
    ) AS pay_setup_complete_with_uid,
    -- new fxa does not have user id in fxa_pay_setup - engage
    COUNTIF(
      pay_setup_complete_with_uid
      AND pay_setup_engage_with_uid
      AND pay_account_setup_other
    ) AS existing_fxa_signedoff_pay_setup_complete,
  FROM
    flows
  GROUP BY
    partition_date,
    country,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    utm_term,
    entrypoint_experiment,
    entrypoint_variation,
    ua_browser,
    ua_version,
    os_name,
    os_version,
    entrypoint,
    plan_id
)
SELECT
  partition_date,
  country,
  utm_medium,
  utm_source,
  utm_campaign,
  utm_content,
  utm_term,
  entrypoint_experiment,
  entrypoint_variation,
  ua_browser,
  ua_version,
  os_name,
  os_version,
  entrypoint,
  plan_id,
  product_id,
  pricing_plan,
  plan_name,
  rp_button_view AS vpn_site_hits,
  mozfun.vpn.channel_group(
    utm_campaign => utm_campaign,
    utm_content => utm_content,
    utm_medium => utm_medium,
    utm_source => utm_source
  ) AS channel_group,
  pay_account_setup_view + existing_fxa_signedin_pay_setup_view AS total_acquisition_process_start,
  pay_setup_engage_with_uid + pay_setup_engage_without_uid AS total_payment_setup_engage,
  pay_setup_complete_without_uid + pay_setup_complete_with_uid AS total_payment_setup_complete,
  SUM(rp_button_view) OVER partition_date AS overall_total_vpn_site_hits,
  SUM(
    pay_account_setup_view + existing_fxa_signedin_pay_setup_view
  ) OVER partition_date AS overall_total_acquisition_process_start,
  SUM(
    pay_setup_complete_without_uid + pay_setup_complete_with_uid
  ) OVER partition_date AS overall_total_payment_setup_complete,
  pay_account_setup_engage - pay_account_setup_engage_other AS new_fxa_user_input_emails,
  pay_setup_engage_without_uid AS new_fxa_payment_setup_engage,
  pay_setup_complete_without_uid AS new_fxa_payment_setup_complete,
  SUM(pay_account_setup_engage) OVER partition_date - SUM(
    pay_account_setup_engage_other
  ) OVER partition_date AS overall_new_fxa_user_input_emails,
  SUM(pay_setup_complete_without_uid) OVER partition_date AS overall_new_fxa_payment_setup_complete,
  pay_setup_view_with_uid AS existing_fxa_payment_setup_view,
  pay_setup_engage_with_uid AS existing_fxa_payment_setup_engage,
  pay_setup_complete_with_uid AS existing_fxa_payment_setup_complete,
  existing_fxa_signedin_pay_setup_view AS existing_fxa_signedin_payment_setup_view,
  pay_setup_engage_with_uid - existing_fxa_signedoff_pay_setup_engage AS existing_fxa_signedin_payment_setup_engage,
  pay_setup_complete_with_uid - existing_fxa_signedoff_pay_setup_complete AS existing_fxa_signedin_payment_setup_complete,
  SUM(pay_setup_complete_with_uid) OVER partition_date - SUM(
    existing_fxa_signedoff_pay_setup_complete
  ) OVER partition_date AS overall_existing_fxa_signedin_payment_setup_complete,
  SUM(
    existing_fxa_signedin_pay_setup_view
  ) OVER partition_date AS overall_existing_fxa_signedin_payment_setup_view,
  pay_account_setup_other AS existing_fxa_signedoff_signin_cta_click,
  SUM(
    pay_account_setup_other
  ) OVER partition_date AS overall_existing_fxa_signedoff_signin_cta_click,
  pay_setup_view_with_uid - existing_fxa_signedin_pay_setup_view AS existing_signedoff_fxa_payment_setup_view,
  existing_fxa_signedoff_pay_setup_engage AS existing_fxa_signedoff_payment_setup_engage,
  existing_fxa_signedoff_pay_setup_complete AS existing_fxa_signedoff_payment_setup_complete,
  SUM(
    existing_fxa_signedoff_pay_setup_complete
  ) OVER partition_date AS overall_existing_fxa_signedoff_payment_setup_complete,
  SUM(pay_setup_view_with_uid) OVER partition_date - SUM(
    existing_fxa_signedin_pay_setup_view
  ) OVER partition_date AS overall_existing_signedoff_fxa_payment_setup_view,
FROM
  flow_counts
LEFT JOIN
  pricing_plans
USING
  (plan_id)
WINDOW
  partition_date AS (
    PARTITION BY
      partition_date
  )
