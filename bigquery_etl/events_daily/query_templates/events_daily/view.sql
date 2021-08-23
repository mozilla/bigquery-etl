CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.{{ view_dataset }}.events_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.{{ base_dataset }}.events_daily_v1`
