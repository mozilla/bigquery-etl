-- TODO:
--    1 new view + 1 UDF
--    View to collect activity for all apps
--    UDF to calculate active user bits pattern.
--  Benefits:
--    No backfills required unless the view is materialized.
--    The definition lives in one file for all apps.
--  Trade-offs:
--    Introduces a non-standard way to calculate bits pattern, which is non standard and therefore harder to maintain.
--    Lower performance as data is calculated on every run and not materialized.
--    Higher cost: On average 30 new experiments run and would be re-calculating this data. See https://mozilla.cloud.looker.com/dashboards/1540?Date=90%20day
--    Reduced complexity of the lineage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.active_users_view`
AS
WITH active_user_definition AS
(
  SELECT
    submission_date,
    app_name,
    -- TODO: UDF to calculate this bit pattern + tests.
    --   This is where the definition of active user would live.
    active_user_bits(submission_date, durations > 0 AND isp != 'Fenix BrowserStack') AS days_active_user_bits,
  FROM
      `moz-fx-data-shared-prod.fenix.baseline_clients_daily`
  --  TODO: UNION ALL the apps.
)
SELECT
    days_active_user_bits,
    udf.pos_of_trailing_set_bit(days_active_user_bits) = 1 AS is_dau,
    udf.pos_of_trailing_set_bit(days_active_user_bits) <= 7 AS is_wau,
    udf.pos_of_trailing_set_bit(days_active_user_bits) <= 28 AS is_mau,
    IF(app_name='Firefox Desktop', True, False) AS is_desktop,
    IF(app_name!='Firefox Desktop', True, False) AS is_mobile
FROM
  active_user_definition
