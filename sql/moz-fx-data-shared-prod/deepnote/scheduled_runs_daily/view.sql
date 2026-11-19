CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.deepnote.scheduled_runs_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.deepnote_derived.scheduled_runs_daily_v1`
