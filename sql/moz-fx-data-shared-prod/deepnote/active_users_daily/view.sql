CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.deepnote.active_users_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.deepnote_derived.active_users_daily_v1`
