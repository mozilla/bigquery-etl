{{ header }}
-- View for histogram aggregates that handles time-partitioning
CREATE OR REPLACE VIEW
  `{{ project }}.{{ dataset }}.{{ prefix }}__view_clients_daily_histogram_aggregates_v1`
AS
SELECT
  * EXCEPT (submission_date),
  DATE(_PARTITIONTIME) AS submission_date
FROM
  `{{ project }}.{{ dataset }}.{{ prefix }}__clients_daily_histogram_aggregates*`
