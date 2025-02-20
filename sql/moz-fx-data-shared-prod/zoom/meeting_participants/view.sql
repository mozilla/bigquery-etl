CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.zoom.meeting_participants`
AS
SELECT
  meeting_id,
  user_id AS participant_id,
  participant_user_id,
  bo_mtg_id,
  customer_key,
  duration,
  failover,
  join_time,
  leave_time,
  name,
  registrant_id,
  status,
  user_email,
  _fivetran_deleted AS is_deleted,
FROM
  `moz-fx-data-bq-fivetran.zoom.meeting_participant`
