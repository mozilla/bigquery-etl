SELECT
  * EXCEPT (channel)
FROM
  `moz-fx-data-glam-prod-fca7.glam_etl.org_mozilla_fenix_glam_nightly__extract_probe_counts_v1`
WHERE
  -- filter based on https://github.com/mozilla/python_mozaggregator/blob/6c0119bfd0b535346c37cb3f707d998039d3e24b/mozaggregator/service.py#L51
  metric NOT LIKE "%search_counts%"
  AND metric NOT LIKE "%browser_search%"
  AND metric NOT LIKE "%event_counts%"
