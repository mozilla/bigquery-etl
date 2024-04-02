-- extract the relevant fields for each funnel step and segment if necessary
WITH login_overall_success_by_service_login_view AS (
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
    client_info.client_id AS client_id,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'login_view'
),
login_overall_success_by_service_login_success AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  INNER JOIN
    login_overall_success_by_service_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'login_submit_success'
),
login_2fa_by_service_login_view AS (
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
    client_info.client_id AS client_id,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'login_view'
),
login_2fa_by_service_login_2fa_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  INNER JOIN
    login_2fa_by_service_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'login_totp_form_view'
),
login_2fa_by_service_login_2fa_success AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.accounts_events
  INNER JOIN
    login_2fa_by_service_login_2fa_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND metrics.string.event_name = 'login_totp_code_success_view'
),
-- aggregate each funnel step value
login_overall_success_by_service_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_overall_success_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_overall_success_by_service_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
login_overall_success_by_service_login_success_aggregated AS (
  SELECT
    submission_date,
    "login_overall_success_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_overall_success_by_service_login_success
  GROUP BY
    service,
    submission_date,
    funnel
),
login_2fa_by_service_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_2fa_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_2fa_by_service_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
login_2fa_by_service_login_2fa_view_aggregated AS (
  SELECT
    submission_date,
    "login_2fa_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_2fa_by_service_login_2fa_view
  GROUP BY
    service,
    submission_date,
    funnel
),
login_2fa_by_service_login_2fa_success_aggregated AS (
  SELECT
    submission_date,
    "login_2fa_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_2fa_by_service_login_2fa_success
  GROUP BY
    service,
    submission_date,
    funnel
),
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    COALESCE(
      login_overall_success_by_service_login_view_aggregated.service,
      login_2fa_by_service_login_view_aggregated.service
    ) AS service,
    submission_date,
    funnel,
    COALESCE(
      login_overall_success_by_service_login_view_aggregated.aggregated,
      login_2fa_by_service_login_view_aggregated.aggregated
    ) AS login_view,
    COALESCE(NULL, NULL) AS confirm_email,
    COALESCE(NULL, login_2fa_by_service_login_2fa_view_aggregated.aggregated) AS login_2fa_view,
    COALESCE(
      login_overall_success_by_service_login_success_aggregated.aggregated,
      NULL
    ) AS login_success,
    COALESCE(
      NULL,
      login_2fa_by_service_login_2fa_success_aggregated.aggregated
    ) AS login_2fa_success,
  FROM
    login_overall_success_by_service_login_view_aggregated
  FULL OUTER JOIN
    login_overall_success_by_service_login_success_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_2fa_by_service_login_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_2fa_by_service_login_2fa_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    login_2fa_by_service_login_2fa_success_aggregated
    USING (submission_date, service, funnel)
)
SELECT
  *
FROM
  merged_funnels
