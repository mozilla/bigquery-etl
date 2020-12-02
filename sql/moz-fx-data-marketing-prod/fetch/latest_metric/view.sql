CREATE OR REPLACE VIEW
  `moz-fx-data-marketing-prod.fetch.latest_metric`
AS
SELECT
  int64_field_0 AS fetch_id,
  * EXCEPT (int64_field_0),
FROM
  `moz-fx-data-marketing-prod.fetch.metric_*`
WHERE
  _TABLE_SUFFIX = (SELECT MAX(_TABLE_SUFFIX) FROM `moz-fx-data-marketing-prod.fetch.metric_*`)
