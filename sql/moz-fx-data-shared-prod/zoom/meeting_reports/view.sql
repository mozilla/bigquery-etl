CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.zoom.meeting_reports`
AS
SELECT
  uuid,
  meeting_id,
  dept,
  duration,
  end_time,
  participants_count,
  start_time,
  total_minutes,
  type,
  user_email,
  user_name,
  _fivetran_deleted AS is_deleted,
FROM
  `moz-fx-data-bq-fivetran.zoom.meeting_report`
