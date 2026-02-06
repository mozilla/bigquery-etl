CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.firefox_desktop_marketing_funnel_last_year`
AS
SELECT
  funnel.*,
  -- Year-over-year metrics (364 days to align day-of-week)
  COALESCE(funnel_funnel_last_year.visits, 0) AS visits_ly,
  COALESCE(funnel_funnel_last_year.downloads, 0) AS downloads_ly,
  COALESCE(funnel_funnel_last_year.installs, 0) AS installs_ly,
  COALESCE(funnel_funnel_last_year.new_profiles, 0) AS new_profiles_ly,
  COALESCE(funnel_funnel_last_year.return_user, 0) AS return_user_ly,
  COALESCE(funnel_funnel_last_year.retained_week4, 0) AS retained_week4_ly
FROM
  `moz-fx-data-shared-prod.telemetry.firefox_desktop_marketing_funnel` AS funnel
-- Self-join to get last year's metrics for YoY comparison
-- Using 364 days (52 weeks) to align day-of-week for fair comparison
LEFT JOIN
  `moz-fx-data-shared-prod.telemetry.firefox_desktop_marketing_funnel` AS funnel_funnel_last_year
  ON funnel.submission_date = funnel_last_year.submission_date + INTERVAL 364 DAY
  AND funnel.country = funnel_last_year.country
  AND funnel.funnel_derived = funnel_last_year.funnel_derived
  AND COALESCE(funnel.channel_raw, '') = COALESCE(funnel_last_year.channel_raw, '')
  AND COALESCE(funnel.campaign, '') = COALESCE(funnel_last_year.campaign, '')
  AND COALESCE(funnel.campaign_id, '') = COALESCE(funnel_last_year.campaign_id, '')
  AND COALESCE(funnel.source, '') = COALESCE(funnel_last_year.source, '')
  AND COALESCE(funnel.medium, '') = COALESCE(funnel_last_year.medium, '')
  AND COALESCE(funnel.normalized_os, '') = COALESCE(funnel_last_year.normalized_os, '')
  AND COALESCE(funnel.partner_org, '') = COALESCE(funnel_last_year.partner_org, '')
  AND COALESCE(funnel.distribution_model, '') = COALESCE(funnel_last_year.distribution_model, '')
  AND COALESCE(funnel.distribution_id, '') = COALESCE(funnel_last_year.distribution_id, '');
