WITH sends AS (
  SELECT
    send_time,
    dispatch_id,
    message_extras,
  FROM
    `moz-fx-data-shared-prod.braze_external.braze_sends_v1`
  WHERE
    DATE(send_time) = DATE_SUB(@click_time, INTERVAL 30 DAY)
),
opens AS (
  SELECT
    open_id,
    dispatch_id,
    machine_open,
  FROM
    `moz-fx-data-shared-prod.braze_external.braze_opens_v1`
  WHERE
    DATE(open_time) = DATE_SUB(@click_time, INTERVAL 30 DAY)
),
clicks AS (
  SELECT
    click_id,
    click_time,
    user_id,
    dispatch_id,
    external_user_id,
    timezone,
    campaign_id,
    campaign_name,
    message_variation_id,
    message_variation_name,
    email_address,
    url,
    canvas_id,
    canvas_name,
    canvas_variation_id,
    canvas_variation_name,
    canvas_step_id,
    canvas_step_name,
    send_id,
    user_agent,
    ip_pool,
    link_id,
    link_alias,
    esp,
    from_domain,
    is_amp,
    app_group_id,
    device_class,
    device_os,
    device_model,
    browser,
    mailbox_provider,
    is_suspected_bot_click,
    suspected_bot_click_reason,
    fxa_id_sha256,
  FROM
    `moz-fx-data-shared-prod.braze_external.braze_clicks_v1`
  WHERE
    DATE(click_time) = @click_time
)
SELECT
  clicks.*,
  sends.send_time,
  sends.message_extras,
  opens.open_id,
  opens.machine_open
FROM
  clicks
LEFT JOIN
  sends
  ON clicks.dispatch_id = sends.dispatch_id
LEFT JOIN
  opens
  ON clicks.dispatch_id = opens.dispatch_id
