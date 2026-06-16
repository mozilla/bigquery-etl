WITH mozilla_space_emails AS (
  SELECT
    click.id AS click_id,
    TIMESTAMP_SECONDS(click.time) AS click_time,
    click.user_id,
    click.dispatch_id,
    click.external_user_id,
    click.timezone,
    click.campaign_id,
    click.campaign_name,
    click.message_variation_id,
    click.message_variation_name,
    click.email_address,
    click.url,
    click.canvas_id,
    click.canvas_name,
    click.canvas_variation_id,
    click.canvas_variation_name,
    click.canvas_step_id,
    click.canvas_step_name,
    click.send_id,
    click.user_agent,
    click.ip_pool,
    click.link_id,
    click.link_alias,
    click.esp,
    click.from_domain,
    click.is_amp,
    click.app_group_id,
    click.device_class,
    click.device_os,
    click.device_model,
    click.browser,
    click.mailbox_provider,
    click.is_suspected_bot_click,
    click.suspected_bot_click_reason,
  FROM
    `moz-fx-data-shared-prod.braze_external.braze_currents_mozilla_click_v1` AS click
  WHERE
    DATE(TIMESTAMP_SECONDS(click.time)) = @click_time
),
firefox_space_emails AS (
  SELECT
    click.id AS click_id,
    TIMESTAMP_SECONDS(click.time) AS click_time,
    click.user_id,
    click.dispatch_id,
    click.external_user_id,
    click.timezone,
    click.campaign_id,
    click.campaign_name,
    click.message_variation_id,
    click.message_variation_name,
    click.email_address,
    click.url,
    click.canvas_id,
    click.canvas_name,
    click.canvas_variation_id,
    click.canvas_variation_name,
    click.canvas_step_id,
    click.canvas_step_name,
    click.send_id,
    click.user_agent,
    click.ip_pool,
    click.link_id,
    click.link_alias,
    click.esp,
    click.from_domain,
    click.is_amp,
    click.app_group_id,
    click.device_class,
    click.device_os,
    click.device_model,
    click.browser,
    click.mailbox_provider,
    click.is_suspected_bot_click,
    click.suspected_bot_click_reason,
  FROM
    `moz-fx-data-shared-prod.braze_external.braze_currents_firefox_click_v1` AS click
  WHERE
    DATE(TIMESTAMP_SECONDS(click.time)) = @click_time
),
unioned AS (
  SELECT
    *
  FROM
    mozilla_space_emails
  UNION ALL
  SELECT
    *
  FROM
    firefox_space_emails
)
SELECT
  unioned.*,
  COALESCE(users.fxa_id_sha256, win10_users.fxa_id_sha256) AS fxa_id_sha256
FROM
  unioned
LEFT JOIN
  `moz-fx-data-shared-prod.braze_derived.users_v1` AS users
  ON users.external_id = unioned.external_user_id
LEFT JOIN
  `moz-fx-data-shared-prod.braze_derived.fxa_win10_users_historical_v1` AS win10_users
  ON win10_users.external_id = unioned.external_user_id
WHERE
  DATE(click_time) = @click_time
