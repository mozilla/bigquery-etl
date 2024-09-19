-- extract the relevant fields for each funnel step and segment if necessary
WITH delete_account_account_pref_view AS (
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
    AND event = 'account_pref.view'
    AND metrics.string.session_flow_id != ''
),
delete_account_delete_account_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    delete_account_account_pref_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'delete_account.settings_submit'
    AND metrics.string.session_flow_id != ''
),
add_two_factor_account_pref_view AS (
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
    AND event = 'account_pref.view'
    AND metrics.string.session_flow_id != ''
),
add_two_factor_add_two_factor_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    add_two_factor_account_pref_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'account_pref.two_step_auth_submit'
    AND metrics.string.session_flow_id != ''
),
device_signout_account_pref_view AS (
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
    AND event = 'account_pref.view'
    AND metrics.string.session_flow_id != ''
),
device_signout_device_signout_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    device_signout_account_pref_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'account_pref.device_signout'
    AND metrics.string.session_flow_id != ''
),
google_play_account_pref_view AS (
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
    AND event = 'account_pref.view'
    AND metrics.string.session_flow_id != ''
),
google_play_google_play_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    google_play_account_pref_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'account_pref.google_play_submit'
    AND metrics.string.session_flow_id != ''
),
apple_store_account_pref_view AS (
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
    AND event = 'account_pref.view'
    AND metrics.string.session_flow_id != ''
),
apple_store_apple_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    apple_store_account_pref_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'account_pref.apple_submit'
    AND metrics.string.session_flow_id != ''
),
google_unlink_account_pref_view AS (
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
    AND event = 'account_pref.view'
    AND metrics.string.session_flow_id != ''
),
google_unlink_google_unlink_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    google_unlink_account_pref_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'account_pref.google_unlink_submit'
    AND metrics.string.session_flow_id != ''
),
apple_unlink_account_pref_view AS (
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
    AND event = 'account_pref.view'
    AND metrics.string.session_flow_id != ''
),
apple_unlink_apple_unlink_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    apple_unlink_account_pref_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'account_pref.apple_unlink_submit'
    AND metrics.string.session_flow_id != ''
),
google_unlink_confirm_account_pref_view AS (
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
    AND event = 'account_pref.view'
    AND metrics.string.session_flow_id != ''
),
google_unlink_confirm_google_unlink_confirm_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    google_unlink_confirm_account_pref_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'account_pref.google_unlink_submit_confirm'
    AND metrics.string.session_flow_id != ''
),
apple_unlink_confirm_account_pref_view AS (
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
    AND event = 'account_pref.view'
    AND metrics.string.session_flow_id != ''
),
apple_unlink_confirm_apple_unlink_confirm_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    apple_unlink_confirm_account_pref_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'account_pref.apple_unlink_submit_confirm'
    AND metrics.string.session_flow_id != ''
),
change_password_account_pref_view AS (
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
    AND event = 'account_pref.view'
    AND metrics.string.session_flow_id != ''
),
change_password_change_password_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    change_password_account_pref_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'account_pref.change_password_submit'
    AND metrics.string.session_flow_id != ''
),
secondary_email_account_pref_view AS (
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
    AND event = 'account_pref.view'
    AND metrics.string.session_flow_id != ''
),
secondary_email_secondary_email_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    secondary_email_account_pref_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'account_pref.secondary_email_submit'
    AND metrics.string.session_flow_id != ''
),
display_name_account_pref_view AS (
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
    AND event = 'account_pref.view'
    AND metrics.string.session_flow_id != ''
),
display_name_display_name_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    display_name_account_pref_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'account_pref.display_name_submit'
    AND metrics.string.session_flow_id != ''
),
recovery_key_account_pref_view AS (
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
    AND event = 'account_pref.view'
    AND metrics.string.session_flow_id != ''
),
recovery_key_recovery_key_engage AS (
  SELECT
    metrics.string.session_flow_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    metrics.string.account_user_id_sha256 AS client_id_column,
    metrics.string.session_flow_id AS column
  FROM
    mozdata.accounts_frontend.events_stream
  INNER JOIN
    recovery_key_account_pref_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = metrics.string.session_flow_id
  WHERE
    {% if is_init() %}
      DATE(submission_timestamp) >= DATE("2024-07-25")
    {% else %}
      DATE(submission_timestamp) = @submission_date
    {% endif %}
    AND event = 'account_pref.recovery_key_submit'
    AND metrics.string.session_flow_id != ''
),
-- aggregate each funnel step value
delete_account_account_pref_view_aggregated AS (
  SELECT
    submission_date,
    "delete_account" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    delete_account_account_pref_view
  GROUP BY
    submission_date,
    funnel
),
delete_account_delete_account_engage_aggregated AS (
  SELECT
    submission_date,
    "delete_account" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    delete_account_delete_account_engage
  GROUP BY
    submission_date,
    funnel
),
add_two_factor_account_pref_view_aggregated AS (
  SELECT
    submission_date,
    "add_two_factor" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    add_two_factor_account_pref_view
  GROUP BY
    submission_date,
    funnel
),
add_two_factor_add_two_factor_engage_aggregated AS (
  SELECT
    submission_date,
    "add_two_factor" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    add_two_factor_add_two_factor_engage
  GROUP BY
    submission_date,
    funnel
),
device_signout_account_pref_view_aggregated AS (
  SELECT
    submission_date,
    "device_signout" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    device_signout_account_pref_view
  GROUP BY
    submission_date,
    funnel
),
device_signout_device_signout_engage_aggregated AS (
  SELECT
    submission_date,
    "device_signout" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    device_signout_device_signout_engage
  GROUP BY
    submission_date,
    funnel
),
google_play_account_pref_view_aggregated AS (
  SELECT
    submission_date,
    "google_play" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    google_play_account_pref_view
  GROUP BY
    submission_date,
    funnel
),
google_play_google_play_engage_aggregated AS (
  SELECT
    submission_date,
    "google_play" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    google_play_google_play_engage
  GROUP BY
    submission_date,
    funnel
),
apple_store_account_pref_view_aggregated AS (
  SELECT
    submission_date,
    "apple_store" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    apple_store_account_pref_view
  GROUP BY
    submission_date,
    funnel
),
apple_store_apple_engage_aggregated AS (
  SELECT
    submission_date,
    "apple_store" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    apple_store_apple_engage
  GROUP BY
    submission_date,
    funnel
),
google_unlink_account_pref_view_aggregated AS (
  SELECT
    submission_date,
    "google_unlink" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    google_unlink_account_pref_view
  GROUP BY
    submission_date,
    funnel
),
google_unlink_google_unlink_engage_aggregated AS (
  SELECT
    submission_date,
    "google_unlink" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    google_unlink_google_unlink_engage
  GROUP BY
    submission_date,
    funnel
),
apple_unlink_account_pref_view_aggregated AS (
  SELECT
    submission_date,
    "apple_unlink" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    apple_unlink_account_pref_view
  GROUP BY
    submission_date,
    funnel
),
apple_unlink_apple_unlink_engage_aggregated AS (
  SELECT
    submission_date,
    "apple_unlink" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    apple_unlink_apple_unlink_engage
  GROUP BY
    submission_date,
    funnel
),
google_unlink_confirm_account_pref_view_aggregated AS (
  SELECT
    submission_date,
    "google_unlink_confirm" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    google_unlink_confirm_account_pref_view
  GROUP BY
    submission_date,
    funnel
),
google_unlink_confirm_google_unlink_confirm_engage_aggregated AS (
  SELECT
    submission_date,
    "google_unlink_confirm" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    google_unlink_confirm_google_unlink_confirm_engage
  GROUP BY
    submission_date,
    funnel
),
apple_unlink_confirm_account_pref_view_aggregated AS (
  SELECT
    submission_date,
    "apple_unlink_confirm" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    apple_unlink_confirm_account_pref_view
  GROUP BY
    submission_date,
    funnel
),
apple_unlink_confirm_apple_unlink_confirm_engage_aggregated AS (
  SELECT
    submission_date,
    "apple_unlink_confirm" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    apple_unlink_confirm_apple_unlink_confirm_engage
  GROUP BY
    submission_date,
    funnel
),
change_password_account_pref_view_aggregated AS (
  SELECT
    submission_date,
    "change_password" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    change_password_account_pref_view
  GROUP BY
    submission_date,
    funnel
),
change_password_change_password_engage_aggregated AS (
  SELECT
    submission_date,
    "change_password" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    change_password_change_password_engage
  GROUP BY
    submission_date,
    funnel
),
secondary_email_account_pref_view_aggregated AS (
  SELECT
    submission_date,
    "secondary_email" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    secondary_email_account_pref_view
  GROUP BY
    submission_date,
    funnel
),
secondary_email_secondary_email_engage_aggregated AS (
  SELECT
    submission_date,
    "secondary_email" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    secondary_email_secondary_email_engage
  GROUP BY
    submission_date,
    funnel
),
display_name_account_pref_view_aggregated AS (
  SELECT
    submission_date,
    "display_name" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    display_name_account_pref_view
  GROUP BY
    submission_date,
    funnel
),
display_name_display_name_engage_aggregated AS (
  SELECT
    submission_date,
    "display_name" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    display_name_display_name_engage
  GROUP BY
    submission_date,
    funnel
),
recovery_key_account_pref_view_aggregated AS (
  SELECT
    submission_date,
    "recovery_key" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    recovery_key_account_pref_view
  GROUP BY
    submission_date,
    funnel
),
recovery_key_recovery_key_engage_aggregated AS (
  SELECT
    submission_date,
    "recovery_key" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    recovery_key_recovery_key_engage
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
      delete_account_account_pref_view_aggregated.aggregated,
      add_two_factor_account_pref_view_aggregated.aggregated,
      device_signout_account_pref_view_aggregated.aggregated,
      google_play_account_pref_view_aggregated.aggregated,
      apple_store_account_pref_view_aggregated.aggregated,
      google_unlink_account_pref_view_aggregated.aggregated,
      apple_unlink_account_pref_view_aggregated.aggregated,
      google_unlink_confirm_account_pref_view_aggregated.aggregated,
      apple_unlink_confirm_account_pref_view_aggregated.aggregated,
      change_password_account_pref_view_aggregated.aggregated,
      secondary_email_account_pref_view_aggregated.aggregated,
      display_name_account_pref_view_aggregated.aggregated,
      recovery_key_account_pref_view_aggregated.aggregated
    ) AS account_pref_view,
    COALESCE(
      delete_account_delete_account_engage_aggregated.aggregated,
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
      NULL
    ) AS delete_account_engage,
    COALESCE(
      NULL,
      add_two_factor_add_two_factor_engage_aggregated.aggregated,
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
    ) AS add_two_factor_engage,
    COALESCE(
      NULL,
      NULL,
      device_signout_device_signout_engage_aggregated.aggregated,
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
    ) AS device_signout_engage,
    COALESCE(
      NULL,
      NULL,
      NULL,
      google_play_google_play_engage_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS google_play_engage,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      apple_store_apple_engage_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS apple_engage,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      google_unlink_google_unlink_engage_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS google_unlink_engage,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      apple_unlink_apple_unlink_engage_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS apple_unlink_engage,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      google_unlink_confirm_google_unlink_confirm_engage_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS google_unlink_confirm_engage,
    COALESCE(
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      NULL,
      apple_unlink_confirm_apple_unlink_confirm_engage_aggregated.aggregated,
      NULL,
      NULL,
      NULL,
      NULL
    ) AS apple_unlink_confirm_engage,
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
      change_password_change_password_engage_aggregated.aggregated,
      NULL,
      NULL,
      NULL
    ) AS change_password_engage,
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
      secondary_email_secondary_email_engage_aggregated.aggregated,
      NULL,
      NULL
    ) AS secondary_email_engage,
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
      display_name_display_name_engage_aggregated.aggregated,
      NULL
    ) AS display_name_engage,
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
      recovery_key_recovery_key_engage_aggregated.aggregated
    ) AS recovery_key_engage,
  FROM
    delete_account_account_pref_view_aggregated
  FULL OUTER JOIN
    delete_account_delete_account_engage_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    add_two_factor_account_pref_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    add_two_factor_add_two_factor_engage_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    device_signout_account_pref_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    device_signout_device_signout_engage_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    google_play_account_pref_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    google_play_google_play_engage_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    apple_store_account_pref_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    apple_store_apple_engage_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    google_unlink_account_pref_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    google_unlink_google_unlink_engage_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    apple_unlink_account_pref_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    apple_unlink_apple_unlink_engage_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    google_unlink_confirm_account_pref_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    google_unlink_confirm_google_unlink_confirm_engage_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    apple_unlink_confirm_account_pref_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    apple_unlink_confirm_apple_unlink_confirm_engage_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    change_password_account_pref_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    change_password_change_password_engage_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    secondary_email_account_pref_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    secondary_email_secondary_email_engage_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    display_name_account_pref_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    display_name_display_name_engage_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    recovery_key_account_pref_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    recovery_key_recovery_key_engage_aggregated
    USING (submission_date, funnel)
)
SELECT
  *
FROM
  merged_funnels
