CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.{{ app_id }}.event_types`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.{{ app_id }}_derived.event_types_v1`
