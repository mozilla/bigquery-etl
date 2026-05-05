CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.zoom.meetings`
AS
SELECT
  uuid,
  id,
  host_id,
  created_at,
  duration,
  start_time,
  timezone,
  type,
  _fivetran_deleted AS is_deleted,
FROM
  `moz-fx-data-bq-fivetran.zoom.meeting`
