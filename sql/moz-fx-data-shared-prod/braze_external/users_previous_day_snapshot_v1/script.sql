DROP SNAPSHOT TABLE `moz-fx-data-shared-prod.braze_external.users_previous_day_snapshot_v1`;

CREATE SNAPSHOT TABLE `moz-fx-data-shared-prod.braze_external.users_previous_day_snapshot_v1` CLONE `moz-fx-data-shared-prod.braze_derived.users_v1` FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(
  CURRENT_TIMESTAMP(),
  INTERVAL 1 HOUR
);

  -- Chose 1 hour here to ensure it captures the state before the current run
  -- if we reduce latency in the future we will need to reduce this
