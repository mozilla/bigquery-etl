CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.adjust.adjust_kpi_deliverables`
AS
SELECT
    date,
    app,
    network,
    network_token,
    campaign,
    campaign_token,
    adgroup,
    adgroup_token,
    creative,
    creative_token,
    country,
    os,
    device,
    clicks,
    installs,
    limit_ad_tracking_install_rate,
    click_conversion_rate,
    impression_conversion_rate,
    sessions,
    daus,
    waus,
    maus
FROM `moz-fx-data-shared-prod.adjust_derived.adjust_deliverables_v1`
