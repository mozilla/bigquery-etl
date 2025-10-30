-- Generated via ./bqetl generate glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozregression.events_stream`
AS
WITH events_stream_union AS (
  SELECT
    "org_mozilla_mozregression" AS normalized_app_id,
    e.*
  FROM
    `moz-fx-data-shared-prod.org_mozilla_mozregression_derived.events_stream_v1` AS e
)
SELECT
  *,
FROM
  events_stream_union
