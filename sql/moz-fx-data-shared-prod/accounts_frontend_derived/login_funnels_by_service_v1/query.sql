-- extract the relevant fields for each funnel step and segment if necessary
WITH login_complete_by_service_login_view AS (
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
login_complete_by_service_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    login_complete_by_service_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.complete'
    AND metrics.string.session_flow_id != ''
),
login_submit_complete_by_service_login_view AS (
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
login_submit_complete_by_service_login_submit AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    login_submit_complete_by_service_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.submit'
    AND metrics.string.session_flow_id != ''
),
login_submit_complete_by_service_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    login_submit_complete_by_service_login_submit AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.complete'
    AND metrics.string.session_flow_id != ''
),
login_email_confirmation_complete_by_service_login_view AS (
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
login_email_confirmation_complete_by_service_login_submit AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    login_email_confirmation_complete_by_service_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.submit'
    AND metrics.string.session_flow_id != ''
),
login_email_confirmation_complete_by_service_login_email_confirmation_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    login_email_confirmation_complete_by_service_login_submit AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.email_confirmation_view'
    AND metrics.string.session_flow_id != ''
),
login_email_confirmation_complete_by_service_login_email_confirmation_submit AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    login_email_confirmation_complete_by_service_login_email_confirmation_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.email_confirmation_submit'
    AND metrics.string.session_flow_id != ''
),
login_email_confirmation_complete_by_service_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    login_email_confirmation_complete_by_service_login_email_confirmation_submit AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.complete'
    AND metrics.string.session_flow_id != ''
),
login_2fa_complete_by_service_login_view AS (
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
login_2fa_complete_by_service_login_submit AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    login_2fa_complete_by_service_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.submit'
    AND metrics.string.session_flow_id != ''
),
login_2fa_complete_by_service_login_two_factor_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    login_2fa_complete_by_service_login_submit AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.totp_form_view'
    AND metrics.string.session_flow_id != ''
),
login_2fa_complete_by_service_login_two_factor_submit AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    login_2fa_complete_by_service_login_two_factor_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.totp_code_submit'
    AND metrics.string.session_flow_id != ''
),
login_2fa_complete_by_service_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    login_2fa_complete_by_service_login_two_factor_submit AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.complete'
    AND metrics.string.session_flow_id != ''
),
logins_from_google_email_first_email_first_view AS (
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
logins_from_google_email_first_email_first_google_start AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    logins_from_google_email_first_email_first_view AS prev
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
logins_from_google_email_first_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    logins_from_google_email_first_email_first_google_start AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.complete'
    AND metrics.string.session_flow_id != ''
),
logins_from_google_email_first_google_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    logins_from_google_email_first_login_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.google_login_complete'
    AND metrics.string.session_flow_id != ''
),
logins_from_apple_email_first_email_first_view AS (
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
logins_from_apple_email_first_email_first_apple_start AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    logins_from_apple_email_first_email_first_view AS prev
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
logins_from_apple_email_first_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    logins_from_apple_email_first_email_first_apple_start AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.complete'
    AND metrics.string.session_flow_id != ''
),
logins_from_apple_email_first_apple_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    logins_from_apple_email_first_login_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.apple_login_complete'
    AND metrics.string.session_flow_id != ''
),
logins_from_google_reg_reg_view AS (
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
logins_from_google_reg_reg_google_start AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    logins_from_google_reg_reg_view AS prev
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
logins_from_google_reg_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    logins_from_google_reg_reg_google_start AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.complete'
    AND metrics.string.session_flow_id != ''
),
logins_from_google_reg_google_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    logins_from_google_reg_login_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.google_login_complete'
    AND metrics.string.session_flow_id != ''
),
logins_from_apple_reg_reg_view AS (
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
logins_from_apple_reg_reg_apple_start AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    logins_from_apple_reg_reg_view AS prev
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
logins_from_apple_reg_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    logins_from_apple_reg_reg_apple_start AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.complete'
    AND metrics.string.session_flow_id != ''
),
logins_from_apple_reg_apple_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    logins_from_apple_reg_login_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.apple_login_complete'
    AND metrics.string.session_flow_id != ''
),
logins_from_google_login_login_view AS (
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
logins_from_google_login_login_google_start AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    logins_from_google_login_login_view AS prev
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
logins_from_google_login_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    logins_from_google_login_login_google_start AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.complete'
    AND metrics.string.session_flow_id != ''
),
logins_from_google_login_google_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    logins_from_google_login_login_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.google_login_complete'
    AND metrics.string.session_flow_id != ''
),
logins_from_apple_login_login_view AS (
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
logins_from_apple_login_login_apple_start AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    logins_from_apple_login_login_view AS prev
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
logins_from_apple_login_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    logins_from_apple_login_login_apple_start AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.complete'
    AND metrics.string.session_flow_id != ''
),
logins_from_apple_login_apple_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    logins_from_apple_login_login_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.apple_login_complete'
    AND metrics.string.session_flow_id != ''
),
login_from_google_deeplink_google_deeplink AS (
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
login_from_google_deeplink_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    login_from_google_deeplink_google_deeplink AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.complete'
    AND metrics.string.session_flow_id != ''
),
login_from_google_deeplink_google_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    login_from_google_deeplink_login_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.google_login_complete'
    AND metrics.string.session_flow_id != ''
),
logins_from_apple_deeplink_apple_deeplink AS (
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
logins_from_apple_deeplink_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    logins_from_apple_deeplink_apple_deeplink AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.complete'
    AND metrics.string.session_flow_id != ''
),
logins_from_apple_deeplink_apple_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    logins_from_apple_deeplink_login_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.apple_login_complete'
    AND metrics.string.session_flow_id != ''
),
login_from_google_cached_logins_cached_login_view AS (
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
    AND event = 'cached_login.view'
    AND metrics.string.session_flow_id != ''
),
login_from_google_cached_logins_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    login_from_google_cached_logins_cached_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.complete'
    AND metrics.string.session_flow_id != ''
),
login_from_google_cached_logins_google_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    login_from_google_cached_logins_login_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.google_login_complete'
    AND metrics.string.session_flow_id != ''
),
logins_from_apple_cached_logins_cached_login_view AS (
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
    AND event = 'cached_login.view'
    AND metrics.string.session_flow_id != ''
),
logins_from_apple_cached_logins_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    logins_from_apple_cached_logins_cached_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.complete'
    AND metrics.string.session_flow_id != ''
),
logins_from_apple_cached_logins_apple_login_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    logins_from_apple_cached_logins_login_complete AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-01-01")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.apple_login_complete'
    AND metrics.string.session_flow_id != ''
),
-- aggregate each funnel step value
login_complete_by_service_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_complete_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_complete_by_service_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
login_complete_by_service_login_complete_aggregated AS (
  SELECT
    submission_date,
    "login_complete_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_complete_by_service_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
login_submit_complete_by_service_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_submit_complete_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_submit_complete_by_service_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
login_submit_complete_by_service_login_submit_aggregated AS (
  SELECT
    submission_date,
    "login_submit_complete_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_submit_complete_by_service_login_submit
  GROUP BY
    service,
    submission_date,
    funnel
),
login_submit_complete_by_service_login_complete_aggregated AS (
  SELECT
    submission_date,
    "login_submit_complete_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_submit_complete_by_service_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
login_email_confirmation_complete_by_service_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_email_confirmation_complete_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_email_confirmation_complete_by_service_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
login_email_confirmation_complete_by_service_login_submit_aggregated AS (
  SELECT
    submission_date,
    "login_email_confirmation_complete_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_email_confirmation_complete_by_service_login_submit
  GROUP BY
    service,
    submission_date,
    funnel
),
login_email_confirmation_complete_by_service_login_email_confirmation_view_aggregated AS (
  SELECT
    submission_date,
    "login_email_confirmation_complete_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_email_confirmation_complete_by_service_login_email_confirmation_view
  GROUP BY
    service,
    submission_date,
    funnel
),
login_email_confirmation_complete_by_service_login_email_confirmation_submit_aggregated AS (
  SELECT
    submission_date,
    "login_email_confirmation_complete_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_email_confirmation_complete_by_service_login_email_confirmation_submit
  GROUP BY
    service,
    submission_date,
    funnel
),
login_email_confirmation_complete_by_service_login_complete_aggregated AS (
  SELECT
    submission_date,
    "login_email_confirmation_complete_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_email_confirmation_complete_by_service_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
login_2fa_complete_by_service_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_2fa_complete_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_2fa_complete_by_service_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
login_2fa_complete_by_service_login_submit_aggregated AS (
  SELECT
    submission_date,
    "login_2fa_complete_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_2fa_complete_by_service_login_submit
  GROUP BY
    service,
    submission_date,
    funnel
),
login_2fa_complete_by_service_login_two_factor_view_aggregated AS (
  SELECT
    submission_date,
    "login_2fa_complete_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_2fa_complete_by_service_login_two_factor_view
  GROUP BY
    service,
    submission_date,
    funnel
),
login_2fa_complete_by_service_login_two_factor_submit_aggregated AS (
  SELECT
    submission_date,
    "login_2fa_complete_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_2fa_complete_by_service_login_two_factor_submit
  GROUP BY
    service,
    submission_date,
    funnel
),
login_2fa_complete_by_service_login_complete_aggregated AS (
  SELECT
    submission_date,
    "login_2fa_complete_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_2fa_complete_by_service_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_google_email_first_email_first_view_aggregated AS (
  SELECT
    submission_date,
    "logins_from_google_email_first" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_google_email_first_email_first_view
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_google_email_first_email_first_google_start_aggregated AS (
  SELECT
    submission_date,
    "logins_from_google_email_first" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_google_email_first_email_first_google_start
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_google_email_first_login_complete_aggregated AS (
  SELECT
    submission_date,
    "logins_from_google_email_first" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_google_email_first_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_google_email_first_google_login_complete_aggregated AS (
  SELECT
    submission_date,
    "logins_from_google_email_first" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_google_email_first_google_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_email_first_email_first_view_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_email_first" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_email_first_email_first_view
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_email_first_email_first_apple_start_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_email_first" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_email_first_email_first_apple_start
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_email_first_login_complete_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_email_first" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_email_first_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_email_first_apple_login_complete_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_email_first" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_email_first_apple_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_google_reg_reg_view_aggregated AS (
  SELECT
    submission_date,
    "logins_from_google_reg" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_google_reg_reg_view
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_google_reg_reg_google_start_aggregated AS (
  SELECT
    submission_date,
    "logins_from_google_reg" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_google_reg_reg_google_start
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_google_reg_login_complete_aggregated AS (
  SELECT
    submission_date,
    "logins_from_google_reg" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_google_reg_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_google_reg_google_login_complete_aggregated AS (
  SELECT
    submission_date,
    "logins_from_google_reg" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_google_reg_google_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_reg_reg_view_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_reg" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_reg_reg_view
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_reg_reg_apple_start_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_reg" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_reg_reg_apple_start
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_reg_login_complete_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_reg" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_reg_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_reg_apple_login_complete_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_reg" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_reg_apple_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_google_login_login_view_aggregated AS (
  SELECT
    submission_date,
    "logins_from_google_login" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_google_login_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_google_login_login_google_start_aggregated AS (
  SELECT
    submission_date,
    "logins_from_google_login" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_google_login_login_google_start
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_google_login_login_complete_aggregated AS (
  SELECT
    submission_date,
    "logins_from_google_login" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_google_login_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_google_login_google_login_complete_aggregated AS (
  SELECT
    submission_date,
    "logins_from_google_login" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_google_login_google_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_login_login_view_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_login" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_login_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_login_login_apple_start_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_login" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_login_login_apple_start
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_login_login_complete_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_login" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_login_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_login_apple_login_complete_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_login" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_login_apple_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
login_from_google_deeplink_google_deeplink_aggregated AS (
  SELECT
    submission_date,
    "login_from_google_deeplink" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_from_google_deeplink_google_deeplink
  GROUP BY
    service,
    submission_date,
    funnel
),
login_from_google_deeplink_login_complete_aggregated AS (
  SELECT
    submission_date,
    "login_from_google_deeplink" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_from_google_deeplink_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
login_from_google_deeplink_google_login_complete_aggregated AS (
  SELECT
    submission_date,
    "login_from_google_deeplink" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_from_google_deeplink_google_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_deeplink_apple_deeplink_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_deeplink" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_deeplink_apple_deeplink
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_deeplink_login_complete_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_deeplink" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_deeplink_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_deeplink_apple_login_complete_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_deeplink" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_deeplink_apple_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
login_from_google_cached_logins_cached_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_from_google_cached_logins" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_from_google_cached_logins_cached_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
login_from_google_cached_logins_login_complete_aggregated AS (
  SELECT
    submission_date,
    "login_from_google_cached_logins" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_from_google_cached_logins_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
login_from_google_cached_logins_google_login_complete_aggregated AS (
  SELECT
    submission_date,
    "login_from_google_cached_logins" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_from_google_cached_logins_google_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_cached_logins_cached_login_view_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_cached_logins" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_cached_logins_cached_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_cached_logins_login_complete_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_cached_logins" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_cached_logins_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
logins_from_apple_cached_logins_apple_login_complete_aggregated AS (
  SELECT
    submission_date,
    "logins_from_apple_cached_logins" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    logins_from_apple_cached_logins_apple_login_complete
  GROUP BY
    service,
    submission_date,
    funnel
),
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    COALESCE(
      login_complete_by_service_login_view_aggregated.service,
      login_submit_complete_by_service_login_view_aggregated.service,
      login_email_confirmation_complete_by_service_login_view_aggregated.service,
      login_2fa_complete_by_service_login_view_aggregated.service,
      logins_from_google_email_first_email_first_view_aggregated.service,
      logins_from_apple_email_first_email_first_view_aggregated.service,
      logins_from_google_reg_reg_view_aggregated.service,
      logins_from_apple_reg_reg_view_aggregated.service,
      logins_from_google_login_login_view_aggregated.service,
      logins_from_apple_login_login_view_aggregated.service,
      login_from_google_deeplink_google_deeplink_aggregated.service,
      logins_from_apple_deeplink_apple_deeplink_aggregated.service,
      login_from_google_cached_logins_cached_login_view_aggregated.service,
      logins_from_apple_cached_logins_cached_login_view_aggregated.service
    ) AS service,
    submission_date,
    funnel,
    COALESCE(
      login_complete_by_service_login_view_aggregated.aggregated,
      login_submit_complete_by_service_login_view_aggregated.aggregated,
      login_email_confirmation_complete_by_service_login_view_aggregated.aggregated,
      login_2fa_complete_by_service_login_view_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      logins_from_google_login_login_view_aggregated.aggregated,
      logins_from_apple_login_login_view_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS login_view,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      logins_from_google_email_first_email_first_view_aggregated.aggregated,
      logins_from_apple_email_first_email_first_view_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
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
      NULL,
      logins_from_google_reg_reg_view_aggregated.aggregated,
      logins_from_apple_reg_reg_view_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS reg_view,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      login_from_google_cached_logins_cached_login_view_aggregated.aggregated,
      logins_from_apple_cached_logins_cached_login_view_aggregated.aggregated
    ) AS cached_login_view,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      logins_from_google_email_first_email_first_google_start_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
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
      NULL,
      logins_from_apple_email_first_email_first_apple_start_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
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
      logins_from_google_reg_reg_google_start_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
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
      logins_from_apple_reg_reg_apple_start_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS reg_apple_start,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      logins_from_google_login_login_google_start_aggregated.aggregated,
      NULL,
      NULL,
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
      NULL,
      NULL,
      NULL,
      logins_from_apple_login_login_apple_start_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS login_apple_start,
    COALESCE(
      NULL,
      login_submit_complete_by_service_login_submit_aggregated.aggregated,
      login_email_confirmation_complete_by_service_login_submit_aggregated.aggregated,
      login_2fa_complete_by_service_login_submit_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS login_submit,
    COALESCE(
      NULL,
      NULL,
      login_email_confirmation_complete_by_service_login_email_confirmation_view_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS login_email_confirmation_view,
    COALESCE(
      NULL,
      NULL,
      login_email_confirmation_complete_by_service_login_email_confirmation_submit_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS login_email_confirmation_submit,
    COALESCE(
      NULL,
      NULL,
      NULL,
      login_2fa_complete_by_service_login_two_factor_view_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS login_two_factor_view,
    COALESCE(
      NULL,
      NULL,
      NULL,
      login_2fa_complete_by_service_login_two_factor_submit_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS login_two_factor_submit,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      login_from_google_deeplink_google_deeplink_aggregated.aggregated,
      NULL,
      NULL,
      NULL
    ) AS google_deeplink,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      logins_from_apple_deeplink_apple_deeplink_aggregated.aggregated,
      NULL,
      NULL
    ) AS apple_deeplink,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      logins_from_google_email_first_google_login_complete_aggregated.aggregated,
      NULL,
      logins_from_google_reg_google_login_complete_aggregated.aggregated,
      NULL,
      logins_from_google_login_google_login_complete_aggregated.aggregated,
      NULL,
      login_from_google_deeplink_google_login_complete_aggregated.aggregated,
      NULL,
      login_from_google_cached_logins_google_login_complete_aggregated.aggregated,
      NULL
    ) AS google_login_complete,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      logins_from_apple_email_first_apple_login_complete_aggregated.aggregated,
      NULL,
      logins_from_apple_reg_apple_login_complete_aggregated.aggregated,
      NULL,
      logins_from_apple_login_apple_login_complete_aggregated.aggregated,
      NULL,
      logins_from_apple_deeplink_apple_login_complete_aggregated.aggregated,
      NULL,
      logins_from_apple_cached_logins_apple_login_complete_aggregated.aggregated
    ) AS apple_login_complete,
    COALESCE(
      login_complete_by_service_login_complete_aggregated.aggregated,
      login_submit_complete_by_service_login_complete_aggregated.aggregated,
      login_email_confirmation_complete_by_service_login_complete_aggregated.aggregated,
      login_2fa_complete_by_service_login_complete_aggregated.aggregated,
      logins_from_google_email_first_login_complete_aggregated.aggregated,
      logins_from_apple_email_first_login_complete_aggregated.aggregated,
      logins_from_google_reg_login_complete_aggregated.aggregated,
      logins_from_apple_reg_login_complete_aggregated.aggregated,
      logins_from_google_login_login_complete_aggregated.aggregated,
      logins_from_apple_login_login_complete_aggregated.aggregated,
      login_from_google_deeplink_login_complete_aggregated.aggregated,
      logins_from_apple_deeplink_login_complete_aggregated.aggregated,
      login_from_google_cached_logins_login_complete_aggregated.aggregated,
      logins_from_apple_cached_logins_login_complete_aggregated.aggregated
    ) AS login_complete,
  FROM
    login_complete_by_service_login_view_aggregated
  FULL OUTER JOIN
    login_complete_by_service_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_submit_complete_by_service_login_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_submit_complete_by_service_login_submit_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_submit_complete_by_service_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_email_confirmation_complete_by_service_login_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_email_confirmation_complete_by_service_login_submit_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_email_confirmation_complete_by_service_login_email_confirmation_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_email_confirmation_complete_by_service_login_email_confirmation_submit_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_email_confirmation_complete_by_service_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_2fa_complete_by_service_login_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_2fa_complete_by_service_login_submit_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_2fa_complete_by_service_login_two_factor_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_2fa_complete_by_service_login_two_factor_submit_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_2fa_complete_by_service_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_google_email_first_email_first_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_google_email_first_email_first_google_start_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_google_email_first_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_google_email_first_google_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_email_first_email_first_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_email_first_email_first_apple_start_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_email_first_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_email_first_apple_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_google_reg_reg_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_google_reg_reg_google_start_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_google_reg_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_google_reg_google_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_reg_reg_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_reg_reg_apple_start_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_reg_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_reg_apple_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_google_login_login_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_google_login_login_google_start_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_google_login_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_google_login_google_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_login_login_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_login_login_apple_start_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_login_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_login_apple_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_from_google_deeplink_google_deeplink_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_from_google_deeplink_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_from_google_deeplink_google_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_deeplink_apple_deeplink_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_deeplink_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_deeplink_apple_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_from_google_cached_logins_cached_login_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_from_google_cached_logins_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_from_google_cached_logins_google_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_cached_logins_cached_login_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_cached_logins_login_complete_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    logins_from_apple_cached_logins_apple_login_complete_aggregated
    USING (submission_date, service, funnel)
)
SELECT
  *
FROM
  merged_funnels
