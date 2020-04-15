-- View to union daily scalar aggregates with date partitioning
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.{{ dataset }}.{{ prefix }}__view_clients_daily_scalar_aggregates_v1`
AS
SELECT
  * EXCEPT (submission_date),
  DATE(_PARTITIONTIME) AS submission_date
FROM
  `moz-fx-data-shared-prod.{{ dataset }}.{{ prefix }}__clients_daily_scalar_aggregates*`
