-- extract the relevant fields for each funnel step and segment if necessary
WITH registration_overall_success_by_service_reg_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    IF(
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      ) = 'sync',
      '5882386c6d801776',
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      )
    ) AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.view'
    AND metrics.string.session_flow_id != ''
),
registration_overall_success_by_service_reg_email_code_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    registration_overall_success_by_service_reg_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.signup_code_view'
    AND metrics.string.session_flow_id != ''
),
registration_overall_success_by_service_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registration_overall_success_by_service_reg_email_code_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.complete'
    AND metrics.string.session_flow_id != ''
),
registrations_from_google_deeplink_google_deeplink AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    IF(
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      ) = 'sync',
      '5882386c6d801776',
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      )
    ) AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.google_deeplink'
    AND metrics.string.session_flow_id != ''
),
registrations_from_google_deeplink_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registrations_from_google_deeplink_google_deeplink AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.complete'
    AND metrics.string.session_flow_id != ''
),
registrations_from_google_deeplink_google_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registrations_from_google_deeplink_reg_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.google_reg_complete'
    AND metrics.string.session_flow_id != ''
),
registrations_from_apple_deeplink_apple_deeplink AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    IF(
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      ) = 'sync',
      '5882386c6d801776',
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      )
    ) AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.apple_deeplink'
    AND metrics.string.session_flow_id != ''
),
registrations_from_apple_deeplink_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registrations_from_apple_deeplink_apple_deeplink AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.complete'
    AND metrics.string.session_flow_id != ''
),
registrations_from_apple_deeplink_apple_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registrations_from_apple_deeplink_reg_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.apple_reg_complete'
    AND metrics.string.session_flow_id != ''
),
registrations_from_google_email_first_email_first_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    IF(
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      ) = 'sync',
      '5882386c6d801776',
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      )
    ) AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'email.first_view'
    AND metrics.string.session_flow_id != ''
),
registrations_from_google_email_first_email_first_google_start AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    registrations_from_google_email_first_email_first_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'email.first_google_oauth_start'
    AND metrics.string.session_flow_id != ''
),
registrations_from_google_email_first_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registrations_from_google_email_first_email_first_google_start AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.complete'
    AND metrics.string.session_flow_id != ''
),
registrations_from_google_email_first_google_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registrations_from_google_email_first_reg_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.google_reg_complete'
    AND metrics.string.session_flow_id != ''
),
registrations_from_apple_email_first_email_first_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    IF(
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      ) = 'sync',
      '5882386c6d801776',
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      )
    ) AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'email.first_view'
    AND metrics.string.session_flow_id != ''
),
registrations_from_apple_email_first_email_first_apple_start AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    registrations_from_apple_email_first_email_first_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'email.first_apple_oauth_start'
    AND metrics.string.session_flow_id != ''
),
registrations_from_apple_email_first_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registrations_from_apple_email_first_email_first_apple_start AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.complete'
    AND metrics.string.session_flow_id != ''
),
registrations_from_apple_email_first_apple_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registrations_from_apple_email_first_reg_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.apple_reg_complete'
    AND metrics.string.session_flow_id != ''
),
registrations_from_google_login_login_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    IF(
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      ) = 'sync',
      '5882386c6d801776',
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      )
    ) AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.view'
    AND metrics.string.session_flow_id != ''
),
registrations_from_google_login_login_google_start AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    registrations_from_google_login_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.google_login_start'
    AND metrics.string.session_flow_id != ''
),
registrations_from_google_login_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registrations_from_google_login_login_google_start AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.complete'
    AND metrics.string.session_flow_id != ''
),
registrations_from_google_login_google_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registrations_from_google_login_reg_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.google_reg_complete'
    AND metrics.string.session_flow_id != ''
),
registrations_from_apple_login_login_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    IF(
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      ) = 'sync',
      '5882386c6d801776',
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      )
    ) AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.view'
    AND metrics.string.session_flow_id != ''
),
registrations_from_apple_login_login_apple_start AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    registrations_from_apple_login_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.apple_login_start'
    AND metrics.string.session_flow_id != ''
),
registrations_from_apple_login_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registrations_from_apple_login_login_apple_start AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.complete'
    AND metrics.string.session_flow_id != ''
),
registrations_from_apple_login_apple_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registrations_from_apple_login_reg_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.apple_reg_complete'
    AND metrics.string.session_flow_id != ''
),
registrations_from_google_reg_reg_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    IF(
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      ) = 'sync',
      '5882386c6d801776',
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      )
    ) AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.view'
    AND metrics.string.session_flow_id != ''
),
registrations_from_google_reg_reg_google_start AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    registrations_from_google_reg_reg_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.google_reg_start'
    AND metrics.string.session_flow_id != ''
),
registrations_from_google_reg_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registrations_from_google_reg_reg_google_start AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.complete'
    AND metrics.string.session_flow_id != ''
),
registrations_from_google_reg_google_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registrations_from_google_reg_reg_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.google_reg_complete'
    AND metrics.string.session_flow_id != ''
),
registrations_from_apple_reg_reg_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    IF(
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      ) = 'sync',
      '5882386c6d801776',
      COALESCE(
        NULLIF(metrics.string.relying_party_oauth_client_id, ''),
        NULLIF(metrics.string.relying_party_service, '')
      )
    ) AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.view'
    AND metrics.string.session_flow_id != ''
),
registrations_from_apple_reg_reg_apple_start AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    registrations_from_apple_reg_reg_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.apple_reg_start'
    AND metrics.string.session_flow_id != ''
),
registrations_from_apple_reg_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registrations_from_apple_reg_reg_apple_start AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.complete'
    AND metrics.string.session_flow_id != ''
),
registrations_from_apple_reg_apple_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registrations_from_apple_reg_reg_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.apple_reg_complete'
    AND metrics.string.session_flow_id != ''
),
-- aggregate each funnel step value
registration_overall_success_by_service_reg_view_aggregated AS (
  SELECT
    submission_date,
    "registration_overall_success_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_overall_success_by_service_reg_view
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_overall_success_by_service_reg_email_code_view_aggregated AS (
  SELECT
    submission_date,
    "registration_overall_success_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_overall_success_by_service_reg_email_code_view
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_overall_success_by_service_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registration_overall_success_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_overall_success_by_service_reg_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_google_deeplink_google_deeplink_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_google_deeplink" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_google_deeplink_google_deeplink
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_google_deeplink_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_google_deeplink" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_google_deeplink_reg_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_google_deeplink_google_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_google_deeplink" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_google_deeplink_google_reg_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_apple_deeplink_apple_deeplink_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_apple_deeplink" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_apple_deeplink_apple_deeplink
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_apple_deeplink_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_apple_deeplink" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_apple_deeplink_reg_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_apple_deeplink_apple_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_apple_deeplink" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_apple_deeplink_apple_reg_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_google_email_first_email_first_view_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_google_email_first" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_google_email_first_email_first_view
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_google_email_first_email_first_google_start_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_google_email_first" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_google_email_first_email_first_google_start
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_google_email_first_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_google_email_first" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_google_email_first_reg_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_google_email_first_google_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_google_email_first" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_google_email_first_google_reg_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_apple_email_first_email_first_view_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_apple_email_first" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_apple_email_first_email_first_view
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_apple_email_first_email_first_apple_start_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_apple_email_first" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_apple_email_first_email_first_apple_start
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_apple_email_first_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_apple_email_first" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_apple_email_first_reg_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_apple_email_first_apple_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_apple_email_first" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_apple_email_first_apple_reg_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_google_login_login_view_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_google_login" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_google_login_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_google_login_login_google_start_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_google_login" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_google_login_login_google_start
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_google_login_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_google_login" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_google_login_reg_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_google_login_google_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_google_login" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_google_login_google_reg_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_apple_login_login_view_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_apple_login" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_apple_login_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_apple_login_login_apple_start_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_apple_login" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_apple_login_login_apple_start
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_apple_login_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_apple_login" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_apple_login_reg_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_apple_login_apple_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_apple_login" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_apple_login_apple_reg_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_google_reg_reg_view_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_google_reg" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_google_reg_reg_view
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_google_reg_reg_google_start_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_google_reg" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_google_reg_reg_google_start
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_google_reg_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_google_reg" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_google_reg_reg_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_google_reg_google_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_google_reg" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_google_reg_google_reg_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_apple_reg_reg_view_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_apple_reg" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_apple_reg_reg_view
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_apple_reg_reg_apple_start_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_apple_reg" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_apple_reg_reg_apple_start
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_apple_reg_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_apple_reg" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_apple_reg_reg_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
registrations_from_apple_reg_apple_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registrations_from_apple_reg" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registrations_from_apple_reg_apple_reg_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    COALESCE(
      registration_overall_success_by_service_reg_view_aggregated.service,
      registrations_from_google_deeplink_google_deeplink_aggregated.service,
      registrations_from_apple_deeplink_apple_deeplink_aggregated.service,
      registrations_from_google_email_first_email_first_view_aggregated.service,
      registrations_from_apple_email_first_email_first_view_aggregated.service,
      registrations_from_google_login_login_view_aggregated.service,
      registrations_from_apple_login_login_view_aggregated.service,
      registrations_from_google_reg_reg_view_aggregated.service,
      registrations_from_apple_reg_reg_view_aggregated.service
    ) AS service,
    submission_date,
    funnel,
    COALESCE(
      registration_overall_success_by_service_reg_view_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      registrations_from_google_reg_reg_view_aggregated.aggregated,
      registrations_from_apple_reg_reg_view_aggregated.aggregated
    ) AS reg_view,
    COALESCE(
      NULL,
      NULL,
      NULL,
      registrations_from_google_email_first_email_first_view_aggregated.aggregated,
      registrations_from_apple_email_first_email_first_view_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS email_first_view,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      registrations_from_google_login_login_view_aggregated.aggregated,
      registrations_from_apple_login_login_view_aggregated.aggregated,
      NULL,
      NULL
    ) AS login_view,
    COALESCE(
      NULL,
      NULL,
      NULL,
      registrations_from_google_email_first_email_first_google_start_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS email_first_google_start,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      registrations_from_apple_email_first_email_first_apple_start_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS email_first_apple_start,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      registrations_from_google_reg_reg_google_start_aggregated.aggregated,
      NULL
    ) AS reg_google_start,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      registrations_from_apple_reg_reg_apple_start_aggregated.aggregated
    ) AS reg_apple_start,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      registrations_from_google_login_login_google_start_aggregated.aggregated,
      NULL,
      NULL,
      NULL
    ) AS login_google_start,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      registrations_from_apple_login_login_apple_start_aggregated.aggregated,
      NULL,
      NULL
    ) AS login_apple_start,
    COALESCE(
      registration_overall_success_by_service_reg_email_code_view_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS reg_email_code_view,
    COALESCE(
      NULL,
      registrations_from_google_deeplink_google_deeplink_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS google_deeplink,
    COALESCE(
      NULL,
      NULL,
      registrations_from_apple_deeplink_apple_deeplink_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS apple_deeplink,
    COALESCE(
      NULL,
      registrations_from_google_deeplink_google_reg_complete_aggregated.aggregated,
      NULL,
      registrations_from_google_email_first_google_reg_complete_aggregated.aggregated,
      NULL,
      registrations_from_google_login_google_reg_complete_aggregated.aggregated,
      NULL,
      registrations_from_google_reg_google_reg_complete_aggregated.aggregated,
      NULL
    ) AS google_reg_complete,
    COALESCE(
      NULL,
      NULL,
      registrations_from_apple_deeplink_apple_reg_complete_aggregated.aggregated,
      NULL,
      registrations_from_apple_email_first_apple_reg_complete_aggregated.aggregated,
      NULL,
      registrations_from_apple_login_apple_reg_complete_aggregated.aggregated,
      NULL,
      registrations_from_apple_reg_apple_reg_complete_aggregated.aggregated
    ) AS apple_reg_complete,
    COALESCE(
      registration_overall_success_by_service_reg_complete_aggregated.aggregated,
      registrations_from_google_deeplink_reg_complete_aggregated.aggregated,
      registrations_from_apple_deeplink_reg_complete_aggregated.aggregated,
      registrations_from_google_email_first_reg_complete_aggregated.aggregated,
      registrations_from_apple_email_first_reg_complete_aggregated.aggregated,
      registrations_from_google_login_reg_complete_aggregated.aggregated,
      registrations_from_apple_login_reg_complete_aggregated.aggregated,
      registrations_from_google_reg_reg_complete_aggregated.aggregated,
      registrations_from_apple_reg_reg_complete_aggregated.aggregated
    ) AS reg_complete,
  FROM
    registration_overall_success_by_service_reg_view_aggregated
  FULL OUTER JOIN
    registration_overall_success_by_service_reg_email_code_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registration_overall_success_by_service_reg_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_google_deeplink_google_deeplink_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_google_deeplink_reg_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_google_deeplink_google_reg_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_apple_deeplink_apple_deeplink_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_apple_deeplink_reg_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_apple_deeplink_apple_reg_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_google_email_first_email_first_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_google_email_first_email_first_google_start_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_google_email_first_reg_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_google_email_first_google_reg_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_apple_email_first_email_first_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_apple_email_first_email_first_apple_start_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_apple_email_first_reg_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_apple_email_first_apple_reg_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_google_login_login_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_google_login_login_google_start_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_google_login_reg_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_google_login_google_reg_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_apple_login_login_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_apple_login_login_apple_start_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_apple_login_reg_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_apple_login_apple_reg_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_google_reg_reg_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_google_reg_reg_google_start_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_google_reg_reg_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_google_reg_google_reg_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_apple_reg_reg_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_apple_reg_reg_apple_start_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_apple_reg_reg_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registrations_from_apple_reg_apple_reg_complete_aggregated
    USING (submission_date, service, funnel)
)
SELECT
  *
FROM
  merged_funnels
