-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.reference_browser.events_stream`
AS
WITH events_stream_union AS (
  SELECT
    "org_mozilla_reference_browser" AS normalized_app_id,
    e.*
  FROM
    `moz-fx-data-shared-prod.org_mozilla_reference_browser_derived.events_stream_v1` AS e
)
SELECT
  *,
FROM
  events_stream_union
