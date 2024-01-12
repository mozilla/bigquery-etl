CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.windows_10_build_distribution` AS
WITH
  channel_summary AS (
  SELECT
    normalized_channel,
    build_group,
    max(build_number) AS max_build_number,
    sum(count) AS frequency
  FROM
    `moz-fx-data-shared-prod.telemetry.windows_10_aggregate`
  GROUP BY
    normalized_channel,
    build_group
  ORDER BY
    build_group ASC),
  counts AS (
  SELECT
    normalized_channel,
    sum(frequency) AS total
  FROM
    channel_summary
  GROUP BY
    normalized_channel)
SELECT
  normalized_channel,
  build_group,
  (frequency / total) AS frequency,
  frequency AS `count`
FROM
  channel_summary
INNER JOIN
  counts
  USING (normalized_channel)
ORDER BY
  max_build_number ASC
