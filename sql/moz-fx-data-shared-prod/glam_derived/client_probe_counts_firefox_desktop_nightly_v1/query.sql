SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.glam_extract_firefox_nightly_v1`
WHERE
  -- filter based on https://github.com/mozilla/python_mozaggregator/blob/6c0119bfd0b535346c37cb3f707d998039d3e24b/mozaggregator/service.py#L51
  (
    metric NOT LIKE r"%search\_counts%"
    AND metric NOT LIKE r"%browser\_search%"
    AND metric NOT LIKE r"%event\_counts%"
    AND metric NOT LIKE r"%browser\_engagement\_navigation%"
    AND metric NOT LIKE r"%manager\_message\_size%"
    AND metric NOT LIKE r"%dropped\_frames\_proportion%"
  )
