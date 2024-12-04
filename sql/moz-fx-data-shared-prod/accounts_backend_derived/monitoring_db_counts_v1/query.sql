WITH table_counts AS (
  SELECT
    'account_customers' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_account_customers_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'account_groups' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_account_groups_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'account_reset_tokens' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_account_reset_tokens_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'accounts' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_accounts_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'carts' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_carts_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'device_commands' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_device_commands_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'devices' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_devices_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'email_bounces' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_email_bounces_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'emails' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_emails_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'linked_accounts' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_linked_accounts_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'oauth_codes' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_oauth_codes_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'oauth_refresh_tokens' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_oauth_refresh_tokens_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'oauth_tokens' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_oauth_tokens_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'password_change_tokens' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_password_change_tokens_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'password_forgot_tokens' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_password_forgot_tokens_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'paypal_customers' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_paypal_customers_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'recovery_codes' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_recovery_codes_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'security_events' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_security_events_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'sent_emails' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_sent_emails_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'session_tokens' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_session_tokens_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'signin_codes' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_signin_codes_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'totp' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_totp_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'unblock_codes' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_unblock_codes_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'unverified_tokens' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_unverified_tokens_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
  SELECT
    'verification_reminders' AS table_name,
    COUNT(*) AS total_rows
  FROM
    `moz-fx-data-shared-prod.accounts_db_external.fxa_verification_reminders_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
      @as_of_date + 1,
      'UTC'
    )
  UNION ALL
    (
      SELECT
        "accounts_with_secondary_emails" AS table_name,
        COUNT(
          DISTINCT `moz-fx-data-shared-prod.accounts_db_external.fxa_accounts_v1`.uid
        ) AS total_rows
      FROM
        `moz-fx-data-shared-prod.accounts_db_external.fxa_accounts_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
          @as_of_date + 1,
          'UTC'
        )
      JOIN
        `moz-fx-data-shared-prod.accounts_db_external.fxa_emails_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
          @as_of_date + 1,
          'UTC'
        )
        ON `moz-fx-data-shared-prod.accounts_db_external.fxa_accounts_v1`.uid = `moz-fx-data-shared-prod.accounts_db_external.fxa_emails_v1`.uid
      WHERE
        `moz-fx-data-shared-prod.accounts_db_external.fxa_emails_v1`.isPrimary = FALSE
    )
  UNION ALL
    (
      SELECT
        "accounts_with_unverified_emails" AS table_name,
        COUNT(
          DISTINCT `moz-fx-data-shared-prod.accounts_db_external.fxa_accounts_v1`.uid
        ) AS total_rows
      FROM
        `moz-fx-data-shared-prod.accounts_db_external.fxa_accounts_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
          @as_of_date + 1,
          'UTC'
        )
      JOIN
        `moz-fx-data-shared-prod.accounts_db_external.fxa_emails_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
          @as_of_date + 1,
          'UTC'
        )
        ON `moz-fx-data-shared-prod.accounts_db_external.fxa_accounts_v1`.uid = `moz-fx-data-shared-prod.accounts_db_external.fxa_emails_v1`.uid
      WHERE
        `moz-fx-data-shared-prod.accounts_db_external.fxa_emails_v1`.isVerified = FALSE
    )
  UNION ALL
    (
      SELECT
        "accounts_linked_to_google" AS table_name,
        COUNT(uid) AS total_rows
      FROM
        `moz-fx-data-shared-prod.accounts_db_external.fxa_linked_accounts_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
          @as_of_date + 1,
          'UTC'
        )
      WHERE
        providerId = 1 -- see LinkedAccountProviderIds at https://github.com/mozilla/fxa/blob/main/packages/fxa-settings/src/lib/types.ts
    )
  UNION ALL
    (
      SELECT
        "accounts_linked_to_apple" AS table_name,
        COUNT(uid) AS total_rows
      FROM
        `moz-fx-data-shared-prod.accounts_db_external.fxa_linked_accounts_v1` FOR SYSTEM_TIME AS OF TIMESTAMP(
          @as_of_date + 1,
          'UTC'
        )
      WHERE
        providerId = 2 -- see LinkedAccountProviderIds at https://github.com/mozilla/fxa/blob/main/packages/fxa-settings/src/lib/types.ts
    )
)
SELECT
  @as_of_date AS as_of_date,
  table_name,
  total_rows
FROM
  table_counts
