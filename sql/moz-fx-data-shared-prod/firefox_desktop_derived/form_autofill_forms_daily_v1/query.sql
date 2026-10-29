-- Daily form autofill counts per flow (one form interaction), for overall rates
-- that must count each form once.
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
  has_inferred_field,
  has_autocomplete_field,
  COUNT(*) AS flows,
  COUNTIF(has_popup) AS popup_flows,
  COUNTIF(has_popup AND has_autofill) AS popup_autofilled_flows,
  COUNTIF(has_autofill) AS autofilled_flows,
  COUNTIF(is_form_submitted) AS submitted_flows,
  SUM(detected_fields) AS detected_fields,
  SUM(autofilled_fields) AS autofilled_fields,
  SUM(modified_fields) AS modified_fields,
  SUM(net_kept_fields) AS net_kept_fields,
FROM
  `moz-fx-data-shared-prod.firefox_desktop.form_autofill_flows`
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
  has_inferred_field,
  has_autocomplete_field
