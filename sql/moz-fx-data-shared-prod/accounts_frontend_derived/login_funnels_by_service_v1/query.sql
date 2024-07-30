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
    metrics.string.account_user_id_sha256 AS client_id,
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
    metrics.string.account_user_id_sha256 AS client_id,
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
    metrics.string.account_user_id_sha256 AS client_id,
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
    metrics.string.account_user_id_sha256 AS client_id,
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
    metrics.string.account_user_id_sha256 AS client_id,
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
    metrics.string.account_user_id_sha256 AS client_id,
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
    metrics.string.account_user_id_sha256 AS client_id,
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
    metrics.string.account_user_id_sha256 AS client_id,
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
    metrics.string.account_user_id_sha256 AS client_id,
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
    metrics.string.account_user_id_sha256 AS client_id,
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
    metrics.string.account_user_id_sha256 AS client_id,
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
    metrics.string.account_user_id_sha256 AS client_id,
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
    metrics.string.account_user_id_sha256 AS client_id,
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
    metrics.string.account_user_id_sha256 AS client_id,
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
    metrics.string.account_user_id_sha256 AS client_id,
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
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    COALESCE(
      login_complete_by_service_login_view_aggregated.service,
      login_submit_complete_by_service_login_view_aggregated.service,
      login_email_confirmation_complete_by_service_login_view_aggregated.service,
      login_2fa_complete_by_service_login_view_aggregated.service
    ) AS service,
    submission_date,
    funnel,
    COALESCE(
      login_complete_by_service_login_view_aggregated.aggregated,
      login_submit_complete_by_service_login_view_aggregated.aggregated,
      login_email_confirmation_complete_by_service_login_view_aggregated.aggregated,
      login_2fa_complete_by_service_login_view_aggregated.aggregated
    ) AS login_view,
    COALESCE(
      NULL,
      login_submit_complete_by_service_login_submit_aggregated.aggregated,
      login_email_confirmation_complete_by_service_login_submit_aggregated.aggregated,
      login_2fa_complete_by_service_login_submit_aggregated.aggregated
    ) AS login_submit,
    COALESCE(
      NULL,
      NULL,
      login_email_confirmation_complete_by_service_login_email_confirmation_view_aggregated.aggregated,
      NULL
    ) AS login_email_confirmation_view,
    COALESCE(
      NULL,
      NULL,
      login_email_confirmation_complete_by_service_login_email_confirmation_submit_aggregated.aggregated,
      NULL
    ) AS login_email_confirmation_submit,
    COALESCE(
      NULL,
      NULL,
      NULL,
      login_2fa_complete_by_service_login_two_factor_view_aggregated.aggregated
    ) AS login_two_factor_view,
    COALESCE(
      NULL,
      NULL,
      NULL,
      login_2fa_complete_by_service_login_two_factor_submit_aggregated.aggregated
    ) AS login_two_factor_submit,
    COALESCE(
      login_complete_by_service_login_complete_aggregated.aggregated,
      login_submit_complete_by_service_login_complete_aggregated.aggregated,
      login_email_confirmation_complete_by_service_login_complete_aggregated.aggregated,
      login_2fa_complete_by_service_login_complete_aggregated.aggregated
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
)
SELECT
  *
FROM
  merged_funnels
