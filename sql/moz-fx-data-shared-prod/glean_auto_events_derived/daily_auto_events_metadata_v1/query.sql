WITH click_events AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    CONCAT(event, '.', JSON_VALUE(event_extra.id)) AS full_event_name,
    COUNT(*) AS count
  FROM
    `moz-fx-data-shared-prod.accounts_frontend.events_stream` AS e
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_category = 'glean'
    AND event_name = 'element_click'
  GROUP BY
    submission_date,
    full_event_name
),
page_load_events AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    CONCAT(event, '[', JSON_VALUE(event_extra.url), ']') AS full_event_name,
    COUNT(*) AS count
  FROM
    `moz-fx-data-shared-prod.accounts_frontend.events_stream` AS e
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_category = 'glean'
    AND event_name = 'page_load'
    AND JSON_VALUE(event_extra.url) IN (
      "/",
      "/account_recovery_confirm_key",
      "/account_recovery_reset_password",
      "/authorization",
      "/cannot_create_account",
      "/clear",
      "/complete_reset_password",
      "/complete_signin",
      "/confirm_reset_password",
      "/confirm_signup_code",
      "/confirm_totp_reset_password",
      "/connect_another_device",
      "/cookies_disabled",
      "/force_auth",
      "/inline_recovery_key_setup",
      "/inline_recovery_setup",
      "/inline_totp_setup",
      "/legal",
      "/legal/privacy",
      "/legal/terms",
      "/oauth",
      "/oauth/force_auth",
      "/oauth/signin",
      "/oauth/signup",
      "/pair",
      "/pair/failure",
      "/pair/unsupported",
      "/post_verify/third_party_auth/callback",
      "/post_verify/third_party_auth/set_password",
      "/primary_email_verified",
      "/report_signin",
      "/reset_password",
      "/reset_password_confirmed",
      "/reset_password_verified",
      "/reset_password_with_recovery_key_verified",
      "/settings",
      "/settings/account_recovery",
      "/settings/avatar",
      "/settings/avatar/change",
      "/settings/change_password",
      "/settings/clients",
      "/settings/clientsdi",
      "/settings/create_password",
      "/settings/delete_account",
      "/settings/display_name",
      "/settings/emails",
      "/settings/emails/verify",
      "/settings/recent_activity",
      "/settings/recovery_phone/remove",
      "/settings/recovery_phone/setup",
      "/settings/two_step_authentication",
      "/settings/two_step_authentication/replace_codes",
      "/signin",
      "/signin_bounced",
      "/signin_confirmed" "/signin_recovery_choice",
      "/signin_recovery_code",
      "/signin_recovery_phone",
      "/signin_reported",
      "/signin_token_code",
      "/signin_totp_code",
      "/signin_unblock",
      "/signin_verified",
      "/signup",
      "/signup_verified",
      "/subscriptions",
      "/support",
      "/verify_email"
    )
  GROUP BY
    submission_date,
    full_event_name
),
unioned AS (
  SELECT
    *
  FROM
    click_events
  UNION ALL
  SELECT
    *
  FROM
    page_load_events
)
SELECT
  submission_date,
  "accounts_frontend" AS app,
  full_event_name AS event_name,
  count
FROM
  unioned
