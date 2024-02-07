CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.activity_stream.impression_stats_live`
AS
SELECT
  * REPLACE (mozfun.norm.metadata(metadata) AS metadata)
FROM
  -- We explicitly choose v1 for now, but will transition to a unioned view
  -- over v1 and v2 as the new schema is rolled out; see
  -- https://bugzilla.mozilla.org/show_bug.cgi?id=1761790
  `moz-fx-data-shared-prod.activity_stream_live.impression_stats_v1`
