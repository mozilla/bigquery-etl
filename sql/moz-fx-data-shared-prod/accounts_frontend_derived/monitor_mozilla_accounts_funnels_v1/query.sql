-- extract the relevant fields for each funnel step and segment if necessary
WITH login_success_login_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'login.view'
    AND (
      metrics.string.relying_party_service LIKE '%monitor%'
      OR metrics.string.relying_party_oauth_client_id = '802d56ef2a9af9fa'
      OR metrics.string.relying_party_service = '802d56ef2a9af9fa'
    )
),
login_success_login_submit_success AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    login_success_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'login.submit_success'
    AND (
      metrics.string.relying_party_service LIKE '%monitor%'
      OR metrics.string.relying_party_oauth_client_id = '802d56ef2a9af9fa'
      OR metrics.string.relying_party_service = '802d56ef2a9af9fa'
    )
),
registration_success_reg_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'reg.view'
    AND (
      metrics.string.relying_party_service LIKE '%monitor%'
      OR metrics.string.relying_party_oauth_client_id = '802d56ef2a9af9fa'
      OR metrics.string.relying_party_service = '802d56ef2a9af9fa'
    )
),
registration_success_reg_code_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    registration_success_reg_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'reg.signup_code_view'
    AND (
      metrics.string.relying_party_service LIKE '%monitor%'
      OR metrics.string.relying_party_oauth_client_id = '802d56ef2a9af9fa'
      OR metrics.string.relying_party_service = '802d56ef2a9af9fa'
    )
),
registration_success_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    registration_success_reg_code_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'reg.complete'
    AND (
      metrics.string.relying_party_service LIKE '%monitor%'
      OR metrics.string.relying_party_oauth_client_id = '802d56ef2a9af9fa'
      OR metrics.string.relying_party_service = '802d56ef2a9af9fa'
    )
),
email_first_login_success_email_first_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'email.first_view'
    AND (
      metrics.string.relying_party_service LIKE '%monitor%'
      OR metrics.string.relying_party_oauth_client_id = '802d56ef2a9af9fa'
      OR metrics.string.relying_party_service = '802d56ef2a9af9fa'
    )
),
email_first_login_success_login_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    email_first_login_success_email_first_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'login.view'
    AND (
      metrics.string.relying_party_service LIKE '%monitor%'
      OR metrics.string.relying_party_oauth_client_id = '802d56ef2a9af9fa'
      OR metrics.string.relying_party_service = '802d56ef2a9af9fa'
    )
),
email_first_login_success_login_submit_success AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    email_first_login_success_login_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'login.submit_success'
    AND (
      metrics.string.relying_party_service LIKE '%monitor%'
      OR metrics.string.relying_party_oauth_client_id = '802d56ef2a9af9fa'
      OR metrics.string.relying_party_service = '802d56ef2a9af9fa'
    )
),
email_first_registration_success_email_first_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'email.first_view'
    AND (
      metrics.string.relying_party_service LIKE '%monitor%'
      OR metrics.string.relying_party_oauth_client_id = '802d56ef2a9af9fa'
      OR metrics.string.relying_party_service = '802d56ef2a9af9fa'
    )
),
email_first_registration_success_reg_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    email_first_registration_success_email_first_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'reg.view'
    AND (
      metrics.string.relying_party_service LIKE '%monitor%'
      OR metrics.string.relying_party_oauth_client_id = '802d56ef2a9af9fa'
      OR metrics.string.relying_party_service = '802d56ef2a9af9fa'
    )
),
email_first_registration_success_reg_code_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    email_first_registration_success_reg_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'reg.signup_code_view'
    AND (
      metrics.string.relying_party_service LIKE '%monitor%'
      OR metrics.string.relying_party_oauth_client_id = '802d56ef2a9af9fa'
      OR metrics.string.relying_party_service = '802d56ef2a9af9fa'
    )
),
email_first_registration_success_reg_complete AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_id AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    email_first_registration_success_reg_code_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event = 'reg.complete'
    AND (
      metrics.string.relying_party_service LIKE '%monitor%'
      OR metrics.string.relying_party_oauth_client_id = '802d56ef2a9af9fa'
      OR metrics.string.relying_party_service = '802d56ef2a9af9fa'
    )
),
-- aggregate each funnel step value
login_success_login_view_aggregated AS (
  SELECT
    submission_date,
    "login_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_success_login_view
  GROUP BY
    submission_date,
    funnel
),
login_success_login_submit_success_aggregated AS (
  SELECT
    submission_date,
    "login_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    login_success_login_submit_success
  GROUP BY
    submission_date,
    funnel
),
registration_success_reg_view_aggregated AS (
  SELECT
    submission_date,
    "registration_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_success_reg_view
  GROUP BY
    submission_date,
    funnel
),
registration_success_reg_code_view_aggregated AS (
  SELECT
    submission_date,
    "registration_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_success_reg_code_view
  GROUP BY
    submission_date,
    funnel
),
registration_success_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registration_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_success_reg_complete
  GROUP BY
    submission_date,
    funnel
),
email_first_login_success_email_first_view_aggregated AS (
  SELECT
    submission_date,
    "email_first_login_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_login_success_email_first_view
  GROUP BY
    submission_date,
    funnel
),
email_first_login_success_login_view_aggregated AS (
  SELECT
    submission_date,
    "email_first_login_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_login_success_login_view
  GROUP BY
    submission_date,
    funnel
),
email_first_login_success_login_submit_success_aggregated AS (
  SELECT
    submission_date,
    "email_first_login_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_login_success_login_submit_success
  GROUP BY
    submission_date,
    funnel
),
email_first_registration_success_email_first_view_aggregated AS (
  SELECT
    submission_date,
    "email_first_registration_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_registration_success_email_first_view
  GROUP BY
    submission_date,
    funnel
),
email_first_registration_success_reg_view_aggregated AS (
  SELECT
    submission_date,
    "email_first_registration_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_registration_success_reg_view
  GROUP BY
    submission_date,
    funnel
),
email_first_registration_success_reg_code_view_aggregated AS (
  SELECT
    submission_date,
    "email_first_registration_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_registration_success_reg_code_view
  GROUP BY
    submission_date,
    funnel
),
email_first_registration_success_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "email_first_registration_success" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    email_first_registration_success_reg_complete
  GROUP BY
    submission_date,
    funnel
),
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    submission_date,
    funnel,
    COALESCE(
      NULL,
      NULL,
      email_first_login_success_email_first_view_aggregated.aggregated,
      email_first_registration_success_email_first_view_aggregated.aggregated
    ) AS email_first_view,
    COALESCE(
      login_success_login_view_aggregated.aggregated,
      NULL,
      email_first_login_success_login_view_aggregated.aggregated,
      NULL
    ) AS login_view,
    COALESCE(
      login_success_login_submit_success_aggregated.aggregated,
      NULL,
      email_first_login_success_login_submit_success_aggregated.aggregated,
      NULL
    ) AS login_submit_success,
    COALESCE(
      NULL,
      registration_success_reg_view_aggregated.aggregated,
      NULL,
      email_first_registration_success_reg_view_aggregated.aggregated
    ) AS reg_view,
    COALESCE(
      NULL,
      registration_success_reg_code_view_aggregated.aggregated,
      NULL,
      email_first_registration_success_reg_code_view_aggregated.aggregated
    ) AS reg_code_view,
    COALESCE(
      NULL,
      registration_success_reg_complete_aggregated.aggregated,
      NULL,
      email_first_registration_success_reg_complete_aggregated.aggregated
    ) AS reg_complete,
  FROM
    login_success_login_view_aggregated
  FULL OUTER JOIN
    login_success_login_submit_success_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    registration_success_reg_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    registration_success_reg_code_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    registration_success_reg_complete_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    email_first_login_success_email_first_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    email_first_login_success_login_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    email_first_login_success_login_submit_success_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    email_first_registration_success_email_first_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    email_first_registration_success_reg_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    email_first_registration_success_reg_code_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    email_first_registration_success_reg_complete_aggregated
    USING (submission_date, funnel)
)
SELECT
  *
FROM
  merged_funnels
