CREATE OR REPLACE FUNCTION udf.remove_label_from_metric_path(column_path STRING)
RETURNS STRING AS (
  IF(
    STARTS_WITH(column_path, 'metrics.labeled_'),
    -- labeled metric path looks like metrics.labeled_counter.metric_name.label
    (
      SELECT
        ARRAY_TO_STRING(ARRAY_AGG(s ORDER BY offset), '.')
      FROM
        UNNEST(SPLIT(column_path, '.')) AS s
        WITH OFFSET
      WHERE
        offset < 3
    ),
    column_path
  )
);

-- Tests
SELECT
  mozfun.assert.equals(
    "metrics.labeled_counter.abc",
    udf.remove_label_from_metric_path("metrics.labeled_counter.abc.true")
  ),
  mozfun.assert.equals(
    "metrics.labeled_boolean.tb_ui_configuration_pane_visibility",
    udf.remove_label_from_metric_path(
      "metrics.labeled_boolean.tb_ui_configuration_pane_visibility.folder_pane"
    )
  ),
  mozfun.assert.equals(
    "metrics.custom_distribution.update_pref_service_errors_notify.values",
    udf.remove_label_from_metric_path(
      "metrics.custom_distribution.update_pref_service_errors_notify.values"
    )
  ),
