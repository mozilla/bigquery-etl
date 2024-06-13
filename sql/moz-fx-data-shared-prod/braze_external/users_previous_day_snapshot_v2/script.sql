DROP SNAPSHOT TABLE `moz-fx-data-shared-prod.braze_external.users_previous_day_snapshot_v2`;

CREATE SNAPSHOT TABLE `moz-fx-data-shared-prod.braze_external.users_previous_day_snapshot_v2` CLONE `moz-fx-data-shared-prod.braze_derived.users_v1` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(
  CURRENT_TIMESTAMP(),
  INTERVAL 1 HOUR
);

  -- Chose 12 hours here because it's split between when the job last ran and now (yesterday).
