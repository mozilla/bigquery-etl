-- Daily form autofill field performance, by field and detection method.
-- Field-level counts are per (flow, field). Popup counts are per flow, keyed on
-- the field that showed the popup and classified by that field's own detection
-- method.
SELECT
  submission_date,
  normalized_channel,
  app_version_major,
  app_build_date,
  normalized_country_code,
  form_type,
  ml_version,
  -- Provisional threshold (DENG-11568). About 1.3% of release client-days are
  -- above it, and they send about 47% of all autofill events.
  client_daily_event_count > 400 AS is_high_volume_client,
  f.field,
  f.detection_method,
  COUNTIF(f.detection_method IS NOT NULL) AS detected_fields,
  COUNTIF(f.is_autofilled) AS autofilled_fields,
  COUNTIF(f.is_autofilled AND f.is_modified) AS modified_fields,
  COUNTIF(f.is_autofilled AND NOT f.is_modified) AS net_kept_fields,
  COUNTIF(f.is_popup_shown) AS popup_flows,
  COUNTIF(f.is_popup_shown AND has_autofill) AS popup_autofilled_flows,
  COUNTIF(is_form_submitted) AS submitted_flows,
FROM
  `moz-fx-data-shared-prod.firefox_desktop.form_autofill_flows`,
  UNNEST(fields) AS f
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  normalized_channel,
  app_version_major,
  app_build_date,
  normalized_country_code,
  form_type,
  ml_version,
  is_high_volume_client,
  field,
  detection_method
