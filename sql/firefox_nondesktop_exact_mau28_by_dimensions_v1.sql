CREATE OR REPLACE VIEW
  `moz-fx-data-derived-datasets.telemetry.firefox_nondesktop_exact_mau28_by_dimensions_v1`
AS
SELECT
  * EXCEPT (generated_time)
FROM
  `moz-fx-data-derived-datasets.telemetry.firefox_nondesktop_exact_mau28_raw_v1`
