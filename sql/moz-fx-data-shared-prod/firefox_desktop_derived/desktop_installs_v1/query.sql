WITH download_token_info AS (
  SELECT
    s.jsonPayload.fields.dltoken AS attribution_dltoken,
    MIN(DATE(s.`timestamp`)) AS download_date
  FROM
    `moz-fx-stubattribut-prod-32a5.stubattribution_prod.stdout` AS s
  GROUP BY
    s.jsonPayload.fields.dltoken
),
install_ping AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    installer_type,
    version,
    from_msi,
    funnelcake,
    attribution.experiment AS attribution_experiment,
    attribution.variation AS attribution_variation,
    metadata.isp.name AS isp_name,
    metadata.isp.organization AS isp_organization,
    ping_version,
    attribution.campaign AS attribution_campaign,
    attribution.content AS attribution_content,
    attribution.dlsource AS attribution_dlsource,
    attribution.dltoken AS attribution_dltoken,
    attribution.medium AS attribution_medium,
    attribution.source AS attribution_source,
    attribution.ua AS attribution_ua,
    metadata.geo.city,
    metadata.geo.country,
    metadata.geo.subdivision1,
    normalized_country_code,
    locale,
    os_version,
    build_channel,
    build_id,
    update_channel,
    had_old_install,
    old_default,
    old_version,
    manual_download,
    silent,
    user_cancelled,
    succeeded,
    profile_cleanup_prompt,
    profile_cleanup_requested,
    new_default,
    new_launched,
    sample_id,
    COUNT(*) AS install_attempts
  FROM
    `moz-fx-data-shared-prod.telemetry.install`
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    installer_type,
    version,
    from_msi,
    funnelcake,
    attribution.experiment,
    attribution.variation,
    metadata.isp.name,
    metadata.isp.organization,
    ping_version,
    attribution.campaign,
    attribution.content,
    attribution.dlsource,
    attribution.dltoken,
    attribution.medium,
    attribution.source,
    attribution.ua,
    metadata.geo.city,
    metadata.geo.country,
    metadata.geo.subdivision1,
    normalized_country_code,
    locale,
    os_version,
    build_channel,
    build_id,
    update_channel,
    had_old_install,
    old_default,
    old_version,
    manual_download,
    silent,
    user_cancelled,
    succeeded,
    profile_cleanup_prompt,
    profile_cleanup_requested,
    new_default,
    new_launched,
    sample_id
)
SELECT
  install_ping.submission_date,
  install_ping.installer_type,
  install_ping.version,
  install_ping.from_msi,
  install_ping.funnelcake,
  install_ping.attribution_experiment,
  install_ping.attribution_variation,
  install_ping.isp_name,
  install_ping.isp_organization,
  install_ping.ping_version,
  install_ping.attribution_campaign,
  install_ping.attribution_content,
  install_ping.attribution_dlsource,
  install_ping.attribution_dltoken,
  install_ping.attribution_medium,
  install_ping.attribution_source,
  install_ping.attribution_ua,
  install_ping.city,
  install_ping.country,
  install_ping.subdivision1,
  install_ping.normalized_country_code,
  install_ping.locale,
  install_ping.os_version,
  install_ping.build_channel,
  install_ping.build_id,
  install_ping.update_channel,
  install_ping.had_old_install,
  install_ping.old_default,
  install_ping.old_version,
  install_ping.manual_download,
  install_ping.silent,
  install_ping.user_cancelled,
  install_ping.succeeded,
  install_ping.profile_cleanup_prompt,
  install_ping.profile_cleanup_requested,
  install_ping.new_default,
  install_ping.new_launched,
  install_ping.sample_id,
  install_ping.install_attempts,
  CASE
    WHEN succeeded IS TRUE
      THEN install_ping.install_attempts
    ELSE 0
  END AS installs,
  CASE
    WHEN succeeded IS FALSE
      THEN install_ping.install_attempts
    ELSE 0
  END AS unsuccessful_installs,
  download_token_info.download_date AS attribution_dltoken_date
FROM
  install_ping
LEFT JOIN
  download_token_info
  USING (attribution_dltoken)
