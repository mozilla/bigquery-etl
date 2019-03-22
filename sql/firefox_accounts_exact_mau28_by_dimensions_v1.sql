SELECT
  * EXCEPT (generated_time, country),
  -- We normalize country to match the two-digit country codes that appear in
  -- telemetry data, so that this view is compatible with the exact_mau28 views
  -- for desktop and nondesktop.
  CASE country
    WHEN 'United States' THEN 'US'
    WHEN 'France' THEN 'FR'
    WHEN 'Germany' THEN 'DE'
    WHEN 'United Kingdom' THEN 'UK'
    WHEN 'Canada' THEN 'CA'
    ELSE 'Other'
  END AS country
FROM
  `moz-fx-data-derived-datasets.analysis.firefox_accounts_exact_mau28_raw_v1`

-- This is a "live view" and can be updated via bq:
-- bq update --project moz-fx-data-derived-datasets --use_legacy_sql=false --view "$(cat sql/firefox_accounts_exact_mau28_by_dimensions_v1.sql)" analysis.firefox_accounts_exact_mau28_by_dimensions_v1
