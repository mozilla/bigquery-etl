SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.glam_extract_firefox_beta_v1`
WHERE
  -- filter based on https://github.com/mozilla/python_mozaggregator/blob/6c0119bfd0b535346c37cb3f707d998039d3e24b/mozaggregator/service.py#L51
  (
    metric NOT LIKE "%search_counts%"
    AND metric NOT LIKE "%browser_search%"
    AND metric NOT LIKE "%event_counts%"
    AND metric NOT LIKE "%browser_engagement_navigation%"
    AND metric NOT LIKE "%manager_message_size%"
    AND metric NOT LIKE "%dropped_frames_proportion%"
  )
