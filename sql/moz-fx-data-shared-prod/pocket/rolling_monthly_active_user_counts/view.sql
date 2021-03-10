CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.pocket.rolling_monthly_active_user_counts`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.pocket_derived.rolling_monthly_active_user_counts_v1`
