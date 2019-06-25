
--
CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.smoot_usage_all_v1`
AS
SELECT
  *
FROM
  `moz-fx-data-derived-datasets.telemetry.smoot_usage_desktop_v1`
--
UNION ALL
--
SELECT
  *
FROM
  `moz-fx-data-derived-datasets.telemetry.smoot_usage_nondesktop_v1`
