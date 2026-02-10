-- extract the relevant fields for each funnel step and segment if necessary
WITH registration_password_age_engage_reg_view AS (
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
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.view'
    AND metrics.string.session_flow_id != ''
),
registration_password_age_engage_reg_password_age_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    registration_password_age_engage_reg_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.engage'
    AND metrics.string.session_flow_id != ''
),
registration_create_account_submit_reg_view AS (
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
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.view'
    AND metrics.string.session_flow_id != ''
),
registration_create_account_submit_reg_create_account_submit AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    registration_create_account_submit_reg_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.submit'
    AND metrics.string.session_flow_id != ''
),
registration_create_account_submit_success_reg_view AS (
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
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.view'
    AND metrics.string.session_flow_id != ''
),
registration_create_account_submit_success_reg_create_account_submit_success AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    registration_create_account_submit_success_reg_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.submit_success'
    AND metrics.string.session_flow_id != ''
),
registration_change_email_engage_reg_view AS (
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
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.view'
    AND metrics.string.session_flow_id != ''
),
registration_change_email_engage_reg_change_email_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    registration_change_email_engage_reg_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.change_email_link_click'
    AND metrics.string.session_flow_id != ''
),
registration_whydoweask_engage_reg_view AS (
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
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.view'
    AND metrics.string.session_flow_id != ''
),
registration_whydoweask_engage_reg_whydoweask_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    registration_whydoweask_engage_reg_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.why_do_we_ask_link_click'
    AND metrics.string.session_flow_id != ''
),
registration_marketing_engage_reg_view AS (
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
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.view'
    AND metrics.string.session_flow_id != ''
),
registration_marketing_engage_reg_marketing_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    registration_marketing_engage_reg_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.marketing_engage'
    AND metrics.string.session_flow_id != ''
),
registration_cwts_engage_reg_view AS (
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
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.view'
    AND metrics.string.session_flow_id != ''
),
registration_cwts_engage_reg_cwts_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    registration_cwts_engage_reg_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.cwts_engage'
    AND metrics.string.session_flow_id != ''
),
registration_google_engage_reg_view AS (
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
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.view'
    AND metrics.string.session_flow_id != ''
),
registration_google_engage_reg_google_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    registration_google_engage_reg_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.google_reg_start'
    AND metrics.string.session_flow_id != ''
),
registration_apple_engage_reg_view AS (
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
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'reg.view'
    AND metrics.string.session_flow_id != ''
),
registration_apple_engage_reg_apple_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    registration_apple_engage_reg_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-10")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'third_party_auth.apple_reg_start'
    AND metrics.string.session_flow_id != ''
),
-- aggregate each funnel step value
registration_password_age_engage_reg_view_aggregated AS (
  SELECT
    submission_date,
    "registration_password_age_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_password_age_engage_reg_view
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_password_age_engage_reg_password_age_engage_aggregated AS (
  SELECT
    submission_date,
    "registration_password_age_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_password_age_engage_reg_password_age_engage
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_create_account_submit_reg_view_aggregated AS (
  SELECT
    submission_date,
    "registration_create_account_submit" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_create_account_submit_reg_view
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_create_account_submit_reg_create_account_submit_aggregated AS (
  SELECT
    submission_date,
    "registration_create_account_submit" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_create_account_submit_reg_create_account_submit
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_create_account_submit_success_reg_view_aggregated AS (
  SELECT
    submission_date,
    "registration_create_account_submit_success" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_create_account_submit_success_reg_view
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_create_account_submit_success_reg_create_account_submit_success_aggregated AS (
  SELECT
    submission_date,
    "registration_create_account_submit_success" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_create_account_submit_success_reg_create_account_submit_success
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_change_email_engage_reg_view_aggregated AS (
  SELECT
    submission_date,
    "registration_change_email_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_change_email_engage_reg_view
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_change_email_engage_reg_change_email_engage_aggregated AS (
  SELECT
    submission_date,
    "registration_change_email_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_change_email_engage_reg_change_email_engage
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_whydoweask_engage_reg_view_aggregated AS (
  SELECT
    submission_date,
    "registration_whydoweask_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_whydoweask_engage_reg_view
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_whydoweask_engage_reg_whydoweask_engage_aggregated AS (
  SELECT
    submission_date,
    "registration_whydoweask_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_whydoweask_engage_reg_whydoweask_engage
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_marketing_engage_reg_view_aggregated AS (
  SELECT
    submission_date,
    "registration_marketing_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_marketing_engage_reg_view
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_marketing_engage_reg_marketing_engage_aggregated AS (
  SELECT
    submission_date,
    "registration_marketing_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_marketing_engage_reg_marketing_engage
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_cwts_engage_reg_view_aggregated AS (
  SELECT
    submission_date,
    "registration_cwts_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_cwts_engage_reg_view
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_cwts_engage_reg_cwts_engage_aggregated AS (
  SELECT
    submission_date,
    "registration_cwts_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_cwts_engage_reg_cwts_engage
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_google_engage_reg_view_aggregated AS (
  SELECT
    submission_date,
    "registration_google_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_google_engage_reg_view
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_google_engage_reg_google_engage_aggregated AS (
  SELECT
    submission_date,
    "registration_google_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_google_engage_reg_google_engage
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_apple_engage_reg_view_aggregated AS (
  SELECT
    submission_date,
    "registration_apple_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_apple_engage_reg_view
  GROUP BY
    service,
    submission_date,
    funnel
),
registration_apple_engage_reg_apple_engage_aggregated AS (
  SELECT
    submission_date,
    "registration_apple_engage" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_apple_engage_reg_apple_engage
  GROUP BY
    service,
    submission_date,
    funnel
),
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    COALESCE(
      registration_password_age_engage_reg_view_aggregated.service,
      registration_create_account_submit_reg_view_aggregated.service,
      registration_create_account_submit_success_reg_view_aggregated.service,
      registration_change_email_engage_reg_view_aggregated.service,
      registration_whydoweask_engage_reg_view_aggregated.service,
      registration_marketing_engage_reg_view_aggregated.service,
      registration_cwts_engage_reg_view_aggregated.service,
      registration_google_engage_reg_view_aggregated.service,
      registration_apple_engage_reg_view_aggregated.service
    ) AS service,
    submission_date,
    funnel,
    COALESCE(
      registration_password_age_engage_reg_view_aggregated.aggregated,
      registration_create_account_submit_reg_view_aggregated.aggregated,
      registration_create_account_submit_success_reg_view_aggregated.aggregated,
      registration_change_email_engage_reg_view_aggregated.aggregated,
      registration_whydoweask_engage_reg_view_aggregated.aggregated,
      registration_marketing_engage_reg_view_aggregated.aggregated,
      registration_cwts_engage_reg_view_aggregated.aggregated,
      registration_google_engage_reg_view_aggregated.aggregated,
      registration_apple_engage_reg_view_aggregated.aggregated
    ) AS reg_view,
    COALESCE(
      registration_password_age_engage_reg_password_age_engage_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS reg_password_age_engage,
    COALESCE(
      NULL,
      registration_create_account_submit_reg_create_account_submit_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS reg_create_account_submit,
    COALESCE(
      NULL,
      NULL,
      registration_create_account_submit_success_reg_create_account_submit_success_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS reg_create_account_submit_success,
    COALESCE(
      NULL,
      NULL,
      NULL,
      registration_change_email_engage_reg_change_email_engage_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS reg_change_email_engage,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      registration_whydoweask_engage_reg_whydoweask_engage_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS reg_whydoweask_engage,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      registration_marketing_engage_reg_marketing_engage_aggregated.aggregated,
      NULL,
      NULL,
      NULL
    ) AS reg_marketing_engage,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      registration_cwts_engage_reg_cwts_engage_aggregated.aggregated,
      NULL,
      NULL
    ) AS reg_cwts_engage,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      registration_google_engage_reg_google_engage_aggregated.aggregated,
      NULL
    ) AS reg_google_engage,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      registration_apple_engage_reg_apple_engage_aggregated.aggregated
    ) AS reg_apple_engage,
  FROM
    registration_password_age_engage_reg_view_aggregated
  FULL OUTER JOIN
    registration_password_age_engage_reg_password_age_engage_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registration_create_account_submit_reg_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registration_create_account_submit_reg_create_account_submit_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registration_create_account_submit_success_reg_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registration_create_account_submit_success_reg_create_account_submit_success_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registration_change_email_engage_reg_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registration_change_email_engage_reg_change_email_engage_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registration_whydoweask_engage_reg_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registration_whydoweask_engage_reg_whydoweask_engage_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registration_marketing_engage_reg_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registration_marketing_engage_reg_marketing_engage_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registration_cwts_engage_reg_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registration_cwts_engage_reg_cwts_engage_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registration_google_engage_reg_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registration_google_engage_reg_google_engage_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registration_apple_engage_reg_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    registration_apple_engage_reg_apple_engage_aggregated
    USING (submission_date, service, funnel)
)
SELECT
  *
FROM
  merged_funnels
