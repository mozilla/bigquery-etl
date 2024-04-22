--DROP SNAPSHOT TABLE `moz-fx-data-shared-prod.braze_external.users_previous_day_snapshot_v1`;
DROP SNAPSHOT TABLE `moz-fx-data-shared-prod.braze_external.active_subscriptions_v1`;
DROP SNAPSHOT TABLE `moz-fx-data-shared-prod.braze_external.user_profiles_previous_day_snapshot_v1`;
DROP SNAPSHOT TABLE `moz-fx-data-shared-prod.braze_external.test_snapshot`;

CREATE SNAPSHOT TABLE `moz-fx-data-shared-prod.braze_external.users_previous_day_snapshot_v1` CLONE `moz-fx-data-shared-prod.braze_derived.users_v1`
FOR SYSTEM_TIME AS OF
  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 12 HOUR); 
  -- Chose 12 hours here because it's split between when the job last ran and now (yesterday).
