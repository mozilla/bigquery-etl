CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.{{ view_dataset }}.event_types`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.{{ base_dataset }}.event_types_v1`
