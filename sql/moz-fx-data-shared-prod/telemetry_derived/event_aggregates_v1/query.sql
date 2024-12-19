SELECT
  submission_date,
  event_category,
  event_object,
  event_method,
  event_string_value,
  country,
  normalized_channel,
  os,
  os_version,
  -- Installation events
  COUNTIF(event_category = 'installation') AS installation_cnt,
  -- Security UI Protections Popup events
  COUNTIF(
    event_category = 'security.ui.protectionspopup'
    AND event_object = 'etp_toggle_off'
  ) AS etp_toggle_off_cnt,
  COUNTIF(
    event_category = 'security.ui.protectionspopup'
    AND event_object = 'etp_toggle_on'
  ) AS etp_toggle_on_cnt,
  COUNTIF(
    event_category = 'security.ui.protectionspopup'
    AND event_object = 'sitenotworking_link'
  ) AS sitenotworking_link_cnt,
  COUNTIF(
    event_category = 'security.ui.protectionspopup'
    AND event_object = 'send_report_link'
  ) AS send_report_link_cnt,
  COUNTIF(
    event_category = 'security.ui.protectionspopup'
    AND event_object = 'send_report_submit'
  ) AS send_report_submit_cnt,
  -- Security UI Certerror events
  COUNTIF(event_category = 'security.ui.certerror') AS certerror_cnt,
  -- Certerror aboutcerterror load events
  COUNTIF(
    event_category = 'security.ui.certerror'
    AND event_object = 'aboutcerterror'
    AND event_method = 'load'
  ) AS certerror_aboutcerterror_load_cnt,
  -- Certerror aboutcerterror load events with event_string_value set
  COUNTIF(
    event_category = 'security.ui.certerror'
    AND event_object = 'aboutcerterror'
    AND event_method = 'load'
    AND event_string_value IS NOT NULL
  ) AS certerror_aboutcerterror_load_value_cnt,
  -- Intl Browser Language Events
  COUNTIF(event_category = 'intl.ui.browserLanguage') AS browser_language_cnt,
  COUNTIF(
    event_category = 'intl.ui.browserLanguage'
    AND event_method = 'click'
  ) AS browser_language_click_cnt,
  COUNTIF(
    event_category = 'intl.ui.browserLanguage'
    AND event_object = 'language_item'
  ) AS browser_language_language_item_cnt,
  COUNT(1) AS nbr_events
FROM
  `moz-fx-data-shared-prod.telemetry.events`
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  event_category,
  event_object,
  event_method,
  event_string_value,
  country,
  normalized_channel,
  os,
  os_version
