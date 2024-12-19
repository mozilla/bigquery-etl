SELECT
  submission_date,
  COUNTIF(event_method = 'click' AND event_object = 'etp_toggle_off') AS disable_etp_cnt,
  COUNTIF(event_method = 'click' AND event_object = 'etp_toggle_on') AS enable_etp_cnt,
  COUNTIF(
    event_method = 'click'
    AND event_object = 'sitenotworking_link'
  ) AS click_site_not_working,
  COUNTIF(event_method = 'click' AND event_object = 'send_report_link') AS click_report_cnt,
  COUNTIF(event_method = 'click' AND event_object = 'send_report_submit') AS submit_report_cnt,
  COUNTIF(event_method = 'open') AS open_panel
FROM
  `moz-fx-data-shared-prod.telemetry.events`
WHERE
  event_category = 'security.ui.protectionspopup'
  AND submission_date = @submission_date
GROUP BY
  submission_date
