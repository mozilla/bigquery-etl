SELECT
  submission_date_s3 AS submission_date,
  document_id,
  client_id,
  sample_id,
  subsession_start_date,
  normalized_channel,
  _addon.element.*
FROM
  main_summary_v4,
  UNNEST(active_addons.list) AS _addon
WHERE
  client_id IS NOT NULL
  AND submission_date_s3 = @submission_date
