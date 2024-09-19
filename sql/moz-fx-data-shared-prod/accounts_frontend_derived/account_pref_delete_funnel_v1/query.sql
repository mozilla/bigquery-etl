-- extract the relevant fields for each funnel step and segment if necessary
WITH accounts_pref_delete_account_delete_account_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
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
    AND event = 'delete_account.view'
    AND metrics.string.session_flow_id != ''
),
accounts_pref_delete_account_delete_account_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    accounts_pref_delete_account_delete_account_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'delete_account.engage'
    AND metrics.string.session_flow_id != ''
),
accounts_pref_delete_account_delete_account_submit AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    accounts_pref_delete_account_delete_account_engage AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'delete_account.submit'
    AND metrics.string.session_flow_id != ''
),
accounts_pref_delete_account_delete_account_password_view AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    accounts_pref_delete_account_delete_account_submit AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'delete_account.password_view'
    AND metrics.string.session_flow_id != ''
),
accounts_pref_delete_account_delete_account_password_submit AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    accounts_pref_delete_account_delete_account_password_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'delete_account.password_submit'
    AND metrics.string.session_flow_id != ''
),
accounts_pref_delete_account_account_deleted AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_backend.events_stream
  INNER JOIN
    accounts_pref_delete_account_delete_account_password_submit AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'account.delete_complete'
    AND metrics.string.session_flow_id != ''
),
-- aggregate each funnel step value
accounts_pref_delete_account_delete_account_view_aggregated AS (
  SELECT
    submission_date,
    "accounts_pref_delete_account" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    accounts_pref_delete_account_delete_account_view
  GROUP BY
    submission_date,
    funnel
),
accounts_pref_delete_account_delete_account_engage_aggregated AS (
  SELECT
    submission_date,
    "accounts_pref_delete_account" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    accounts_pref_delete_account_delete_account_engage
  GROUP BY
    submission_date,
    funnel
),
accounts_pref_delete_account_delete_account_submit_aggregated AS (
  SELECT
    submission_date,
    "accounts_pref_delete_account" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    accounts_pref_delete_account_delete_account_submit
  GROUP BY
    submission_date,
    funnel
),
accounts_pref_delete_account_delete_account_password_view_aggregated AS (
  SELECT
    submission_date,
    "accounts_pref_delete_account" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    accounts_pref_delete_account_delete_account_password_view
  GROUP BY
    submission_date,
    funnel
),
accounts_pref_delete_account_delete_account_password_submit_aggregated AS (
  SELECT
    submission_date,
    "accounts_pref_delete_account" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    accounts_pref_delete_account_delete_account_password_submit
  GROUP BY
    submission_date,
    funnel
),
accounts_pref_delete_account_account_deleted_aggregated AS (
  SELECT
    submission_date,
    "accounts_pref_delete_account" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    accounts_pref_delete_account_account_deleted
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
      accounts_pref_delete_account_delete_account_view_aggregated.aggregated
    ) AS delete_account_view,
    COALESCE(
      accounts_pref_delete_account_delete_account_engage_aggregated.aggregated
    ) AS delete_account_engage,
    COALESCE(
      accounts_pref_delete_account_delete_account_submit_aggregated.aggregated
    ) AS delete_account_submit,
    COALESCE(
      accounts_pref_delete_account_delete_account_password_view_aggregated.aggregated
    ) AS delete_account_password_view,
    COALESCE(
      accounts_pref_delete_account_delete_account_password_submit_aggregated.aggregated
    ) AS delete_account_password_submit,
    COALESCE(accounts_pref_delete_account_account_deleted_aggregated.aggregated) AS account_deleted,
  FROM
    accounts_pref_delete_account_delete_account_view_aggregated
  FULL OUTER JOIN
    accounts_pref_delete_account_delete_account_engage_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    accounts_pref_delete_account_delete_account_submit_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    accounts_pref_delete_account_delete_account_password_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    accounts_pref_delete_account_delete_account_password_submit_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    accounts_pref_delete_account_account_deleted_aggregated
    USING (submission_date, funnel)
)
SELECT
  *
FROM
  merged_funnels
