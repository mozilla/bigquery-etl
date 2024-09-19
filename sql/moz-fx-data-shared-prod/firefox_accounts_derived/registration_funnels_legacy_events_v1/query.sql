-- extract the relevant fields for each funnel step and segment if necessary
WITH registration_overall_success_by_service_reg_view AS (
  SELECT
    flow_id AS join_key,
    service AS service,
    country AS country,
    DATE(timestamp) AS submission_date,
    user_id AS client_id_column,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  WHERE
    {% if is_init() %}
      DATE(timestamp) >= DATE("2023-01-01")
    {% else %}
      DATE(timestamp) = @submission_date
    {% endif %}
    AND event_type = 'fxa_reg - view'
),
registration_overall_success_by_service_reg_complete AS (
  SELECT
    flow_id AS join_key,
    prev.service AS service,
    prev.country AS country,
    DATE(timestamp) AS submission_date,
    user_id AS client_id_column,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  INNER JOIN
    registration_overall_success_by_service_reg_view AS prev
    ON prev.submission_date = DATE(timestamp)
    AND prev.join_key = flow_id
  WHERE
    {% if is_init() %}
      DATE(timestamp) >= DATE("2023-01-01")
    {% else %}
      DATE(timestamp) = @submission_date
    {% endif %}
    AND event_type = 'fxa_reg - complete'
),
registration_email_confirmation_overall_success_by_service_reg_view AS (
  SELECT
    flow_id AS join_key,
    service AS service,
    country AS country,
    DATE(timestamp) AS submission_date,
    user_id AS client_id_column,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  WHERE
    {% if is_init() %}
      DATE(timestamp) >= DATE("2023-01-01")
    {% else %}
      DATE(timestamp) = @submission_date
    {% endif %}
    AND event_type = 'fxa_reg - view'
),
registration_email_confirmation_overall_success_by_service_reg_email_code_view AS (
  SELECT
    flow_id AS join_key,
    prev.service AS service,
    prev.country AS country,
    DATE(timestamp) AS submission_date,
    user_id AS client_id_column,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  INNER JOIN
    registration_email_confirmation_overall_success_by_service_reg_view AS prev
    ON prev.submission_date = DATE(timestamp)
    AND prev.join_key = flow_id
  WHERE
    {% if is_init() %}
      DATE(timestamp) >= DATE("2023-01-01")
    {% else %}
      DATE(timestamp) = @submission_date
    {% endif %}
    AND event_type = 'fxa_reg - signup_code_view'
),
registration_email_confirmation_overall_success_by_service_reg_complete AS (
  SELECT
    flow_id AS join_key,
    prev.service AS service,
    prev.country AS country,
    DATE(timestamp) AS submission_date,
    user_id AS client_id_column,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  INNER JOIN
    registration_email_confirmation_overall_success_by_service_reg_email_code_view AS prev
    ON prev.submission_date = DATE(timestamp)
    AND prev.join_key = flow_id
  WHERE
    {% if is_init() %}
      DATE(timestamp) >= DATE("2023-01-01")
    {% else %}
      DATE(timestamp) = @submission_date
    {% endif %}
    AND event_type = 'fxa_reg - complete'
),
google_reg_third_party_auth_completions_google_signin_complete AS (
  SELECT
    flow_id AS join_key,
    service AS service,
    country AS country,
    DATE(timestamp) AS submission_date,
    user_id AS client_id_column,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  WHERE
    {% if is_init() %}
      DATE(timestamp) >= DATE("2023-01-01")
    {% else %}
      DATE(timestamp) = @submission_date
    {% endif %}
    AND event_type = 'fxa_third_party_auth - google_signin_complete'
),
google_reg_third_party_auth_completions_reg_complete AS (
  SELECT
    flow_id AS join_key,
    prev.service AS service,
    prev.country AS country,
    DATE(timestamp) AS submission_date,
    user_id AS client_id_column,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  INNER JOIN
    google_reg_third_party_auth_completions_google_signin_complete AS prev
    ON prev.submission_date = DATE(timestamp)
    AND prev.join_key = flow_id
  WHERE
    {% if is_init() %}
      DATE(timestamp) >= DATE("2023-01-01")
    {% else %}
      DATE(timestamp) = @submission_date
    {% endif %}
    AND event_type = 'fxa_reg - complete'
),
google_login_third_party_auth_completions_google_signin_complete AS (
  SELECT
    flow_id AS join_key,
    service AS service,
    country AS country,
    DATE(timestamp) AS submission_date,
    user_id AS client_id_column,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  WHERE
    {% if is_init() %}
      DATE(timestamp) >= DATE("2023-01-01")
    {% else %}
      DATE(timestamp) = @submission_date
    {% endif %}
    AND event_type = 'fxa_third_party_auth - google_signin_complete'
),
google_login_third_party_auth_completions_login_complete AS (
  SELECT
    flow_id AS join_key,
    prev.service AS service,
    prev.country AS country,
    DATE(timestamp) AS submission_date,
    user_id AS client_id_column,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  INNER JOIN
    google_login_third_party_auth_completions_google_signin_complete AS prev
    ON prev.submission_date = DATE(timestamp)
    AND prev.join_key = flow_id
  WHERE
    {% if is_init() %}
      DATE(timestamp) >= DATE("2023-01-01")
    {% else %}
      DATE(timestamp) = @submission_date
    {% endif %}
    AND event_type = 'fxa_login - complete'
),
apple_reg_third_party_auth_completions_apple_signin_complete AS (
  SELECT
    flow_id AS join_key,
    service AS service,
    country AS country,
    DATE(timestamp) AS submission_date,
    user_id AS client_id_column,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  WHERE
    {% if is_init() %}
      DATE(timestamp) >= DATE("2023-01-01")
    {% else %}
      DATE(timestamp) = @submission_date
    {% endif %}
    AND event_type = 'fxa_third_party_auth - apple_signin_complete'
),
apple_reg_third_party_auth_completions_reg_complete AS (
  SELECT
    flow_id AS join_key,
    prev.service AS service,
    prev.country AS country,
    DATE(timestamp) AS submission_date,
    user_id AS client_id_column,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  INNER JOIN
    apple_reg_third_party_auth_completions_apple_signin_complete AS prev
    ON prev.submission_date = DATE(timestamp)
    AND prev.join_key = flow_id
  WHERE
    {% if is_init() %}
      DATE(timestamp) >= DATE("2023-01-01")
    {% else %}
      DATE(timestamp) = @submission_date
    {% endif %}
    AND event_type = 'fxa_reg - complete'
),
apple_login_third_party_auth_completions_apple_signin_complete AS (
  SELECT
    flow_id AS join_key,
    service AS service,
    country AS country,
    DATE(timestamp) AS submission_date,
    user_id AS client_id_column,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  WHERE
    {% if is_init() %}
      DATE(timestamp) >= DATE("2023-01-01")
    {% else %}
      DATE(timestamp) = @submission_date
    {% endif %}
    AND event_type = 'fxa_third_party_auth - apple_signin_complete'
),
apple_login_third_party_auth_completions_login_complete AS (
  SELECT
    flow_id AS join_key,
    prev.service AS service,
    prev.country AS country,
    DATE(timestamp) AS submission_date,
    user_id AS client_id_column,
    flow_id AS column
  FROM
    mozdata.firefox_accounts.fxa_all_events
  INNER JOIN
    apple_login_third_party_auth_completions_apple_signin_complete AS prev
    ON prev.submission_date = DATE(timestamp)
    AND prev.join_key = flow_id
  WHERE
    {% if is_init() %}
      DATE(timestamp) >= DATE("2023-01-01")
    {% else %}
      DATE(timestamp) = @submission_date
    {% endif %}
    AND event_type = 'fxa_login - complete'
),
-- aggregate each funnel step value
registration_overall_success_by_service_reg_view_aggregated AS (
  SELECT
    submission_date,
    "registration_overall_success_by_service" AS funnel,
    service,
    country,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_overall_success_by_service_reg_view
  GROUP BY
    service,
    country,
    submission_date,
    funnel
),
registration_overall_success_by_service_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registration_overall_success_by_service" AS funnel,
    service,
    country,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_overall_success_by_service_reg_complete
  GROUP BY
    service,
    country,
    submission_date,
    funnel
),
registration_email_confirmation_overall_success_by_service_reg_view_aggregated AS (
  SELECT
    submission_date,
    "registration_email_confirmation_overall_success_by_service" AS funnel,
    service,
    country,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_email_confirmation_overall_success_by_service_reg_view
  GROUP BY
    service,
    country,
    submission_date,
    funnel
),
registration_email_confirmation_overall_success_by_service_reg_email_code_view_aggregated AS (
  SELECT
    submission_date,
    "registration_email_confirmation_overall_success_by_service" AS funnel,
    service,
    country,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_email_confirmation_overall_success_by_service_reg_email_code_view
  GROUP BY
    service,
    country,
    submission_date,
    funnel
),
registration_email_confirmation_overall_success_by_service_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "registration_email_confirmation_overall_success_by_service" AS funnel,
    service,
    country,
    COUNT(DISTINCT column) AS aggregated
  FROM
    registration_email_confirmation_overall_success_by_service_reg_complete
  GROUP BY
    service,
    country,
    submission_date,
    funnel
),
google_reg_third_party_auth_completions_google_signin_complete_aggregated AS (
  SELECT
    submission_date,
    "google_reg_third_party_auth_completions" AS funnel,
    service,
    country,
    COUNT(DISTINCT column) AS aggregated
  FROM
    google_reg_third_party_auth_completions_google_signin_complete
  GROUP BY
    service,
    country,
    submission_date,
    funnel
),
google_reg_third_party_auth_completions_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "google_reg_third_party_auth_completions" AS funnel,
    service,
    country,
    COUNT(DISTINCT column) AS aggregated
  FROM
    google_reg_third_party_auth_completions_reg_complete
  GROUP BY
    service,
    country,
    submission_date,
    funnel
),
google_login_third_party_auth_completions_google_signin_complete_aggregated AS (
  SELECT
    submission_date,
    "google_login_third_party_auth_completions" AS funnel,
    service,
    country,
    COUNT(DISTINCT column) AS aggregated
  FROM
    google_login_third_party_auth_completions_google_signin_complete
  GROUP BY
    service,
    country,
    submission_date,
    funnel
),
google_login_third_party_auth_completions_login_complete_aggregated AS (
  SELECT
    submission_date,
    "google_login_third_party_auth_completions" AS funnel,
    service,
    country,
    COUNT(DISTINCT column) AS aggregated
  FROM
    google_login_third_party_auth_completions_login_complete
  GROUP BY
    service,
    country,
    submission_date,
    funnel
),
apple_reg_third_party_auth_completions_apple_signin_complete_aggregated AS (
  SELECT
    submission_date,
    "apple_reg_third_party_auth_completions" AS funnel,
    service,
    country,
    COUNT(DISTINCT column) AS aggregated
  FROM
    apple_reg_third_party_auth_completions_apple_signin_complete
  GROUP BY
    service,
    country,
    submission_date,
    funnel
),
apple_reg_third_party_auth_completions_reg_complete_aggregated AS (
  SELECT
    submission_date,
    "apple_reg_third_party_auth_completions" AS funnel,
    service,
    country,
    COUNT(DISTINCT column) AS aggregated
  FROM
    apple_reg_third_party_auth_completions_reg_complete
  GROUP BY
    service,
    country,
    submission_date,
    funnel
),
apple_login_third_party_auth_completions_apple_signin_complete_aggregated AS (
  SELECT
    submission_date,
    "apple_login_third_party_auth_completions" AS funnel,
    service,
    country,
    COUNT(DISTINCT column) AS aggregated
  FROM
    apple_login_third_party_auth_completions_apple_signin_complete
  GROUP BY
    service,
    country,
    submission_date,
    funnel
),
apple_login_third_party_auth_completions_login_complete_aggregated AS (
  SELECT
    submission_date,
    "apple_login_third_party_auth_completions" AS funnel,
    service,
    country,
    COUNT(DISTINCT column) AS aggregated
  FROM
    apple_login_third_party_auth_completions_login_complete
  GROUP BY
    service,
    country,
    submission_date,
    funnel
),
-- merge all funnels so results can be written into one table
merged_funnels AS (
  SELECT
    COALESCE(
      registration_overall_success_by_service_reg_view_aggregated.service,
      registration_email_confirmation_overall_success_by_service_reg_view_aggregated.service,
      google_reg_third_party_auth_completions_google_signin_complete_aggregated.service,
      google_login_third_party_auth_completions_google_signin_complete_aggregated.service,
      apple_reg_third_party_auth_completions_apple_signin_complete_aggregated.service,
      apple_login_third_party_auth_completions_apple_signin_complete_aggregated.service
    ) AS service,
    COALESCE(
      registration_overall_success_by_service_reg_view_aggregated.country,
      registration_email_confirmation_overall_success_by_service_reg_view_aggregated.country,
      google_reg_third_party_auth_completions_google_signin_complete_aggregated.country,
      google_login_third_party_auth_completions_google_signin_complete_aggregated.country,
      apple_reg_third_party_auth_completions_apple_signin_complete_aggregated.country,
      apple_login_third_party_auth_completions_apple_signin_complete_aggregated.country
    ) AS country,
    submission_date,
    funnel,
    COALESCE(
      registration_overall_success_by_service_reg_view_aggregated.aggregated,
      registration_email_confirmation_overall_success_by_service_reg_view_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS reg_view,
    COALESCE(
      NULL,
      registration_email_confirmation_overall_success_by_service_reg_email_code_view_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS reg_email_code_view,
    COALESCE(
      registration_overall_success_by_service_reg_complete_aggregated.aggregated,
      registration_email_confirmation_overall_success_by_service_reg_complete_aggregated.aggregated,
      google_reg_third_party_auth_completions_reg_complete_aggregated.aggregated,
      NULL,
      apple_reg_third_party_auth_completions_reg_complete_aggregated.aggregated,
      NULL
    ) AS reg_complete,
    COALESCE(
      NULL,
      NULL,
      NULL,
      google_login_third_party_auth_completions_login_complete_aggregated.aggregated,
      NULL,
      apple_login_third_party_auth_completions_login_complete_aggregated.aggregated
    ) AS login_complete,
    COALESCE(
      NULL,
      NULL,
      google_reg_third_party_auth_completions_google_signin_complete_aggregated.aggregated,
      google_login_third_party_auth_completions_google_signin_complete_aggregated.aggregated,
      NULL,
      NULL
    ) AS google_signin_complete,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      apple_reg_third_party_auth_completions_apple_signin_complete_aggregated.aggregated,
      apple_login_third_party_auth_completions_apple_signin_complete_aggregated.aggregated
    ) AS apple_signin_complete,
  FROM
    registration_overall_success_by_service_reg_view_aggregated
  FULL OUTER JOIN
    registration_overall_success_by_service_reg_complete_aggregated
    USING (submission_date, service, country, funnel)
  FULL OUTER JOIN
    registration_email_confirmation_overall_success_by_service_reg_view_aggregated
    USING (submission_date, service, country, funnel)
  FULL OUTER JOIN
    registration_email_confirmation_overall_success_by_service_reg_email_code_view_aggregated
    USING (submission_date, service, country, funnel)
  FULL OUTER JOIN
    registration_email_confirmation_overall_success_by_service_reg_complete_aggregated
    USING (submission_date, service, country, funnel)
  FULL OUTER JOIN
    google_reg_third_party_auth_completions_google_signin_complete_aggregated
    USING (submission_date, service, country, funnel)
  FULL OUTER JOIN
    google_reg_third_party_auth_completions_reg_complete_aggregated
    USING (submission_date, service, country, funnel)
  FULL OUTER JOIN
    google_login_third_party_auth_completions_google_signin_complete_aggregated
    USING (submission_date, service, country, funnel)
  FULL OUTER JOIN
    google_login_third_party_auth_completions_login_complete_aggregated
    USING (submission_date, service, country, funnel)
  FULL OUTER JOIN
    apple_reg_third_party_auth_completions_apple_signin_complete_aggregated
    USING (submission_date, service, country, funnel)
  FULL OUTER JOIN
    apple_reg_third_party_auth_completions_reg_complete_aggregated
    USING (submission_date, service, country, funnel)
  FULL OUTER JOIN
    apple_login_third_party_auth_completions_apple_signin_complete_aggregated
    USING (submission_date, service, country, funnel)
  FULL OUTER JOIN
    apple_login_third_party_auth_completions_login_complete_aggregated
    USING (submission_date, service, country, funnel)
)
SELECT
  *
FROM
  merged_funnels
