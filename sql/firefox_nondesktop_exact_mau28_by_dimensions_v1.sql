SELECT
  * EXCEPT (generated_time)
FROM
  `moz-fx-data-derived-datasets.analysis.firefox_nondesktop_exact_mau28_raw_v1`

-- This is a "live view" and can be updated via bq:
-- bq update --project moz-fx-data-derived-datasets --use_legacy_sql=false --view "$(cat sql/firefox_nondesktop_exact_mau28_by_dimensions_v1.sql)" analysis.firefox_nondesktop_exact_mau28_by_dimensions_v1
