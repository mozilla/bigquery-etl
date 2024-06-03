-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.engagement_clients`
AS
WITH active_users AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    first_seen_date,
    app_name,
    normalized_channel,
    locale,
    country,
    isp,
    app_display_version,
    is_dau,
    is_wau,
    is_mau,
    is_mobile,
  FROM
    `moz-fx-data-shared-prod.fenix.active_users`
),
attribution AS (
  SELECT
    client_id,
    sample_id,
    channel AS normalized_channel,
    NULLIF(play_store_attribution_campaign, "") AS play_store_attribution_campaign,
    NULLIF(play_store_attribution_medium, "") AS play_store_attribution_medium,
    NULLIF(play_store_attribution_source, "") AS play_store_attribution_source,
    NULLIF(meta_attribution_app, "") AS meta_attribution_app,
    NULLIF(install_source, "") AS install_source,
    NULLIF(adjust_ad_group, "") AS adjust_ad_group,
    CASE
      WHEN adjust_network IN ('Google Organic Search', 'Organic')
        THEN 'Organic'
      ELSE NULLIF(adjust_campaign, "")
    END AS adjust_campaign,
    NULLIF(adjust_creative, "") AS adjust_creative,
    NULLIF(adjust_network, "") AS adjust_network,
  FROM
    `moz-fx-data-shared-prod.fenix_derived.firefox_android_clients_v1`
)
SELECT
  submission_date,
  client_id,
  sample_id,
  first_seen_date,
  app_name,
  normalized_channel,
  app_display_version AS app_version,
  locale,
  country,
  isp,
  is_dau,
  is_wau,
  is_mau,
  is_mobile,
  attribution.play_store_attribution_campaign,
  attribution.play_store_attribution_medium,
  attribution.play_store_attribution_source,
  attribution.meta_attribution_app,
  attribution.install_source,
  attribution.adjust_ad_group,
  attribution.adjust_campaign,
  attribution.adjust_creative,
  attribution.adjust_network,
  CASE
    WHEN active_users.submission_date = first_seen_date
      THEN 'new_profile'
    WHEN DATE_DIFF(active_users.submission_date, first_seen_date, DAY)
      BETWEEN 1
      AND 27
      THEN 'repeat_user'
    WHEN DATE_DIFF(active_users.submission_date, first_seen_date, DAY) >= 28
      THEN 'existing_user'
    ELSE 'Unknown'
  END AS lifecycle_stage,
FROM
  active_users
LEFT JOIN
  attribution
  USING (client_id, sample_id, normalized_channel)
