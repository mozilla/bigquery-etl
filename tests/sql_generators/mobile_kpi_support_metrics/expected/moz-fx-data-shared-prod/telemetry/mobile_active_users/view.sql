-- Query generated via `mobile_kpi_support_metrics` SQL generator.
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.mobile_active_users`
AS
SELECT
  *,
  "fenix" AS product_name,
FROM
  `moz-fx-data-shared-prod.fenix.active_users`
UNION ALL
SELECT
  *,
  "focus_android" AS product_name,
FROM
  `moz-fx-data-shared-prod.focus_android.active_users`
UNION ALL
SELECT
  *,
  "klar_android" AS product_name,
FROM
  `moz-fx-data-shared-prod.klar_android.active_users`
UNION ALL
SELECT
  *,
  "firefox_ios" AS product_name,
FROM
  `moz-fx-data-shared-prod.firefox_ios.active_users`
