CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.account_ecosystem.aet_clients_daily`
AS
WITH desktop AS (
  SELECT
    submission_date,
    CAST(NULL AS string) AS canonical_id,
    ecosystem_client_id_hash,
    'desktop' AS service,
    CAST(NULL AS int64) AS event_count,
    duration_sum,
    active_hours_sum,
    scalar_parent_browser_engagement_total_uri_count_sum,
    normalized_channel AS channel,
    normalized_os AS os,
    normalized_country_code AS country,
  FROM
    `moz-fx-data-shared-prod.account_ecosystem_derived.desktop_clients_daily_v1`
),
fxa_logging AS (
  SELECT
    submission_date,
    canonical_id,
    CAST(NULL AS string) AS ecosystem_client_id_hash,
    -- We likely want to replace oauth_client_id with a human-readable service name.
    FORMAT('fxa - %s', oauth_client_id) AS service,
    event_count,
    CAST(NULL AS int64) AS duration_sum,
    CAST(NULL AS int64) AS active_hours_sum,
    CAST(NULL AS int64) AS scalar_parent_browser_engagement_total_uri_count_sum,
    CAST(NULL AS string) AS channel,
    CAST(NULL AS string) AS os,
    country_code AS country,
  FROM
    `moz-fx-data-shared-prod.account_ecosystem_derived.fxa_logging_users_daily_v1`
),
unioned AS (
  SELECT
    *
  FROM
    desktop
  UNION ALL
  SELECT
    *
  FROM
    fxa_logging
)
SELECT
  coalesce(unioned.canonical_id, ecil.canonical_id) AS user_id,
  ecosystem_client_id_hash AS client_id,
  unioned.* EXCEPT (canonical_id, ecosystem_client_id_hash)
FROM
  unioned
LEFT JOIN
  `moz-fx-data-shared-prod.account_ecosystem_derived.ecosystem_client_id_lookup_v1` AS ecil
USING
  (ecosystem_client_id_hash)
