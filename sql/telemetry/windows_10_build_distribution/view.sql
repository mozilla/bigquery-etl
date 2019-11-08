CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.windows_10_build_distribution` AS
WITH
  channel_summary AS (
  SELECT
    normalized_channel,
    build_group,
    max(build_number) oo,
    sum(count) frequency
  FROM
    `moz-fx-data-shared-prod.telemetry.windows_10_aggregate`
  GROUP BY
    1,
    2
  ORDER BY
    2 ASC ),
  counts AS (
  SELECT
    normalized_channel,
    sum(frequency) total
  FROM
    channel_summary
  GROUP BY
    1 )
SELECT
  cs.normalized_channel,
  build_group,
  (frequency / total) frequency,
  frequency count
FROM (channel_summary cs
  INNER JOIN
    counts cc
  ON
    (cs.normalized_channel = cc.normalized_channel))
ORDER BY
  cs.oo ASC
