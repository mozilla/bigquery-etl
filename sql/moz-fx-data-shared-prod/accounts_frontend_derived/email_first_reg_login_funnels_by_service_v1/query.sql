-- extract the relevant fields for each funnel step and segment if necessary
WITH email_first_login_success_by_service_email_first_view AS (
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
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'email.first_view'
),
email_first_login_success_by_service_login_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    email_first_login_success_by_service_email_first_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'login.view'
),
email_first_login_success_by_service_login_success AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    email_first_login_success_by_service_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'login.submit_success'
),
email_first_registration_success_by_service_email_first_view AS (
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
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'email.first_view'
),
email_first_registration_success_by_service_reg_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    email_first_registration_success_by_service_email_first_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'reg.view'
),
email_first_registration_success_by_service_reg_success AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    prev.service AS service,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    email_first_registration_success_by_service_reg_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'reg.complete'
),
-- aggregate each funnel step value
email_first_login_success_by_service_email_first_view_aggregated AS (
  SELECT
    submission_date,
    "email_first_login_success_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_login_success_by_service_email_first_view
  GROUP BY
    service,
    submission_date,
    funnel
),
email_first_login_success_by_service_login_view_aggregated AS (
  SELECT
    submission_date,
    "email_first_login_success_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_login_success_by_service_login_view
  GROUP BY
    service,
    submission_date,
    funnel
),
email_first_login_success_by_service_login_success_aggregated AS (
  SELECT
    submission_date,
    "email_first_login_success_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_login_success_by_service_login_success
  GROUP BY
    service,
    submission_date,
    funnel
),
email_first_registration_success_by_service_email_first_view_aggregated AS (
  SELECT
    submission_date,
    "email_first_registration_success_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_registration_success_by_service_email_first_view
  GROUP BY
    service,
    submission_date,
    funnel
),
email_first_registration_success_by_service_reg_view_aggregated AS (
  SELECT
    submission_date,
    "email_first_registration_success_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_registration_success_by_service_reg_view
  GROUP BY
    service,
    submission_date,
    funnel
),
email_first_registration_success_by_service_reg_success_aggregated AS (
  SELECT
    submission_date,
    "email_first_registration_success_by_service" AS funnel,
    service,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_registration_success_by_service_reg_success
  GROUP BY
    service,
    submission_date,
    funnel
),
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    COALESCE(
      email_first_login_success_by_service_email_first_view_aggregated.service,
      email_first_registration_success_by_service_email_first_view_aggregated.service
    ) AS service,
    submission_date,
    funnel,
    COALESCE(
      email_first_login_success_by_service_email_first_view_aggregated.aggregated,
      email_first_registration_success_by_service_email_first_view_aggregated.aggregated
    ) AS email_first_view,
    COALESCE(
      email_first_login_success_by_service_login_view_aggregated.aggregated,
      NULL
    ) AS login_view,
    COALESCE(
      email_first_login_success_by_service_login_success_aggregated.aggregated,
      NULL
    ) AS login_success,
    COALESCE(
      NULL,
      email_first_registration_success_by_service_reg_view_aggregated.aggregated
    ) AS reg_view,
    COALESCE(
      NULL,
      email_first_registration_success_by_service_reg_success_aggregated.aggregated
    ) AS reg_success,
  FROM
    email_first_login_success_by_service_email_first_view_aggregated
  FULL OUTER JOIN
    email_first_login_success_by_service_login_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    email_first_login_success_by_service_login_success_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    email_first_registration_success_by_service_email_first_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    email_first_registration_success_by_service_reg_view_aggregated
    USING (submission_date, service, funnel)
  FULL OUTER JOIN
    email_first_registration_success_by_service_reg_success_aggregated
    USING (submission_date, service, funnel)
)
SELECT
  *
FROM
  merged_funnels
