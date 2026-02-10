-- extract the relevant fields for each funnel step and segment if necessary
WITH legacy_user_upgrade_intent_legacy_dashboard_view AS (
  SELECT
    client_info.client_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id_column,
    client_info.client_id AS column
  FROM
    mozdata.monitor_frontend.events_unnested
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_category = 'dashboard'
    AND event_name = 'view'
    AND `mozfun.map.get_key`(event_extra, 'path') LIKE '%/user/dashboard%'
    AND `mozfun.map.get_key`(event_extra, 'legacy_user') = 'true'
),
legacy_user_upgrade_intent_upgrade_intent AS (
  SELECT
    client_info.client_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id_column,
    client_info.client_id AS column
  FROM
    mozdata.monitor_frontend.events_unnested
  INNER JOIN
    legacy_user_upgrade_intent_legacy_dashboard_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_category = 'upgrade_intent'
    AND event_name = 'click'
    AND `mozfun.map.get_key`(event_extra, 'path') LIKE '%/user/dashboard%'
),
new_user_upgrade_intent_new_dashboard_view AS (
  SELECT
    client_info.client_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id_column,
    client_info.client_id AS column
  FROM
    mozdata.monitor_frontend.events_unnested
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_category = 'dashboard'
    AND event_name = 'view'
    AND `mozfun.map.get_key`(event_extra, 'path') LIKE '%/user/dashboard%'
    AND `mozfun.map.get_key`(event_extra, 'legacy_user') = 'false'
),
new_user_upgrade_intent_upgrade_intent AS (
  SELECT
    client_info.client_id AS join_key,
    DATE(submission_timestamp) AS submission_date,
    client_info.client_id AS client_id_column,
    client_info.client_id AS column
  FROM
    mozdata.monitor_frontend.events_unnested
  INNER JOIN
    new_user_upgrade_intent_new_dashboard_view AS prev
    ON prev.submission_date = DATE(submission_timestamp)
    AND prev.join_key = client_info.client_id
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_category = 'upgrade_intent'
    AND event_name = 'click'
    AND `mozfun.map.get_key`(event_extra, 'path') LIKE '%/user/dashboard%'
),
-- aggregate each funnel step value
legacy_user_upgrade_intent_legacy_dashboard_view_aggregated AS (
  SELECT
    submission_date,
    "legacy_user_upgrade_intent" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    legacy_user_upgrade_intent_legacy_dashboard_view
  GROUP BY
    submission_date,
    funnel
),
legacy_user_upgrade_intent_upgrade_intent_aggregated AS (
  SELECT
    submission_date,
    "legacy_user_upgrade_intent" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    legacy_user_upgrade_intent_upgrade_intent
  GROUP BY
    submission_date,
    funnel
),
new_user_upgrade_intent_new_dashboard_view_aggregated AS (
  SELECT
    submission_date,
    "new_user_upgrade_intent" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    new_user_upgrade_intent_new_dashboard_view
  GROUP BY
    submission_date,
    funnel
),
new_user_upgrade_intent_upgrade_intent_aggregated AS (
  SELECT
    submission_date,
    "new_user_upgrade_intent" AS funnel,
    COUNT(DISTINCT column) AS aggregated
  FROM
    new_user_upgrade_intent_upgrade_intent
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
      legacy_user_upgrade_intent_legacy_dashboard_view_aggregated.aggregated,
      NULL
    ) AS legacy_dashboard_view,
    COALESCE(
      NULL,
      new_user_upgrade_intent_new_dashboard_view_aggregated.aggregated
    ) AS new_dashboard_view,
    COALESCE(
      legacy_user_upgrade_intent_upgrade_intent_aggregated.aggregated,
      new_user_upgrade_intent_upgrade_intent_aggregated.aggregated
    ) AS upgrade_intent,
  FROM
    legacy_user_upgrade_intent_legacy_dashboard_view_aggregated
  FULL OUTER JOIN
    legacy_user_upgrade_intent_upgrade_intent_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    new_user_upgrade_intent_new_dashboard_view_aggregated
    USING (submission_date, funnel)
  FULL OUTER JOIN
    new_user_upgrade_intent_upgrade_intent_aggregated
    USING (submission_date, funnel)
)
SELECT
  *
FROM
  merged_funnels
