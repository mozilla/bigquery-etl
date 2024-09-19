-- extract the relevant fields for each funnel step and segment if necessary
WITH login_engage_login_view AS (
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
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.view'
    AND metrics.string.session_flow_id != ''
),
login_engage_login_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    login_engage_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.engage'
    AND metrics.string.session_flow_id != ''
),
login_submit_login_view AS (
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
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.view'
    AND metrics.string.session_flow_id != ''
),
login_submit_login_submit AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    login_submit_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.submit'
    AND metrics.string.session_flow_id != ''
),
login_submit_success_login_view AS (
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
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.view'
    AND metrics.string.session_flow_id != ''
),
login_submit_success_login_submit_success AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    login_submit_success_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.submit_success'
    AND metrics.string.session_flow_id != ''
),
login_diff_account_engage_login_view AS (
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
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.view'
    AND metrics.string.session_flow_id != ''
),
login_diff_account_engage_login_diff_account_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    login_diff_account_engage_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.diff_account_link_click'
    AND metrics.string.session_flow_id != ''
),
login_forgot_pw_engage_login_view AS (
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
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.view'
    AND metrics.string.session_flow_id != ''
),
login_forgot_pw_engage_login_forgot_pw_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    login_forgot_pw_engage_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.forgot_pwd_submit'
    AND metrics.string.session_flow_id != ''
),
login_google_engage_login_view AS (
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
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.view'
    AND metrics.string.session_flow_id != ''
),
login_google_engage_login_google_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    login_google_engage_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.google_login_start'
    AND metrics.string.session_flow_id != ''
),
login_apple_engage_login_view AS (
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
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'login.view'
    AND metrics.string.session_flow_id != ''
),
login_apple_engage_login_apple_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    login_apple_engage_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.apple_login_start'
    AND metrics.string.session_flow_id != ''
),
-- aggregate each funnel step value
login_engage_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_engage_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
login_engage_login_engage_aggregated AS (
  SELECT
    submission_date,
    "login_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_engage_login_engage
  GROUP BY
    service,
    submission_date,
    funnel
),
login_submit_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_submit" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_submit_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
login_submit_login_submit_aggregated AS (
  SELECT
    submission_date,
    "login_submit" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_submit_login_submit
  GROUP BY
    service,
    submission_date,
    funnel
),
login_submit_success_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_submit_success" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_submit_success_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
login_submit_success_login_submit_success_aggregated AS (
  SELECT
    submission_date,
    "login_submit_success" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_submit_success_login_submit_success
  GROUP BY
    service,
    submission_date,
    funnel
),
login_diff_account_engage_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_diff_account_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_diff_account_engage_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
login_diff_account_engage_login_diff_account_engage_aggregated AS (
  SELECT
    submission_date,
    "login_diff_account_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_diff_account_engage_login_diff_account_engage
  GROUP BY
    service,
    submission_date,
    funnel
),
login_forgot_pw_engage_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_forgot_pw_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_forgot_pw_engage_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
login_forgot_pw_engage_login_forgot_pw_engage_aggregated AS (
  SELECT
    submission_date,
    "login_forgot_pw_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_forgot_pw_engage_login_forgot_pw_engage
  GROUP BY
    service,
    submission_date,
    funnel
),
login_google_engage_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_google_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_google_engage_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
login_google_engage_login_google_engage_aggregated AS (
  SELECT
    submission_date,
    "login_google_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_google_engage_login_google_engage
  GROUP BY
    service,
    submission_date,
    funnel
),
login_apple_engage_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_apple_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_apple_engage_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
login_apple_engage_login_apple_engage_aggregated AS (
  SELECT
    submission_date,
    "login_apple_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_apple_engage_login_apple_engage
  GROUP BY
    service,
    submission_date,
    funnel
),
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    COALESCE(
      login_engage_login_view_aggregated.service,
      login_submit_login_view_aggregated.service,
      login_submit_success_login_view_aggregated.service,
      login_diff_account_engage_login_view_aggregated.service,
      login_forgot_pw_engage_login_view_aggregated.service,
      login_google_engage_login_view_aggregated.service,
      login_apple_engage_login_view_aggregated.service
    ) AS service,
    submission_date,
    funnel,
    COALESCE(
      login_engage_login_view_aggregated.aggregated,
      login_submit_login_view_aggregated.aggregated,
      login_submit_success_login_view_aggregated.aggregated,
      login_diff_account_engage_login_view_aggregated.aggregated,
      login_forgot_pw_engage_login_view_aggregated.aggregated,
      login_google_engage_login_view_aggregated.aggregated,
      login_apple_engage_login_view_aggregated.aggregated
    ) AS login_view,
    COALESCE(
      login_engage_login_engage_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS login_engage,
    COALESCE(
      NULL,
      login_submit_login_submit_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS login_submit,
    COALESCE(
      NULL,
      NULL,
      login_submit_success_login_submit_success_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS login_submit_success,
    COALESCE(
      NULL,
      NULL,
      NULL,
      login_diff_account_engage_login_diff_account_engage_aggregated.aggregated,
      NULL,
      NULL,
      NULL
    ) AS login_diff_account_engage,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      login_forgot_pw_engage_login_forgot_pw_engage_aggregated.aggregated,
      NULL,
      NULL
    ) AS login_forgot_pw_engage,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      login_google_engage_login_google_engage_aggregated.aggregated,
      NULL
    ) AS login_google_engage,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      login_apple_engage_login_apple_engage_aggregated.aggregated
    ) AS login_apple_engage,
  FROM
    login_engage_login_view_aggregated
  FULL OUTER JOIN
    login_engage_login_engage_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_submit_login_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_submit_login_submit_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_submit_success_login_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_submit_success_login_submit_success_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_diff_account_engage_login_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_diff_account_engage_login_diff_account_engage_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_forgot_pw_engage_login_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_forgot_pw_engage_login_forgot_pw_engage_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_google_engage_login_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_google_engage_login_google_engage_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_apple_engage_login_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_apple_engage_login_apple_engage_aggregated
    USING (submission_date, service, funnel)
)
SELECT
  *
FROM
  merged_funnels
