WITH metrics_ping_metrics AS (
  -- Non-event metric columns from column_size_v1, normalizing dataset_id into an app
  -- (combining channels) and normalizing column_name into a metric_identifier.
  -- Datasets outside the three target apps map to NULL and are excluded below, so this
  -- branch stays symmetric with the event branch (which already filters to the three apps).
  -- column_name is the BigQuery field_path (from column_size_v1). The
  -- metric_identifier normalization mirrors the Glean column hierarchy by depth:
  --   ping_info.*   / client_info.*  -> keep the full path (depth 2)
  --   metadata.*.*                   -> keep the full path (depth 3)
  --   metrics.<type>.<id>            -> keep <id>, the leaf column name (depth 3)
  --   anything else                  -> keep as-is
  -- Paths that don't match their expected depth are set to NULL and excluded below.
  -- NOTE: for the metrics.* case, <id> is Glean's underscore-flattened metric id
  -- (e.g. "addons_has_enabled_addons") -- it already includes BOTH category and name;
  -- it is NOT just the name. The "." is flattened to "_" because this is a BigQuery
  -- column name (where "." denotes struct traversal). The event branch below keeps the
  -- "category.name" dot form instead. See the metric_identifier description in
  -- schema.yaml for the full rationale; do not split this field to recover hierarchy.
  SELECT
    submission_date,
    CASE
      WHEN dataset_id IN ("org_mozilla_ios_firefox_stable", "org_mozilla_ios_firefoxbeta_stable")
        THEN "Firefox for iOS"
      WHEN dataset_id IN (
          "org_mozilla_fenix_stable",
          "org_mozilla_firefox_beta_stable",
          "org_mozilla_firefox_stable"
        )
        THEN "Firefox for Android"
      WHEN dataset_id IN ("firefox_desktop_stable")
        THEN "Firefox for Desktop"
      ELSE NULL
    END AS normalized_app_name,
    CASE
      SPLIT(column_name, ".")[OFFSET(0)]
      WHEN "ping_info"
        THEN IF(ARRAY_LENGTH(SPLIT(column_name, ".")) = 2, column_name, NULL)
      WHEN "client_info"
        THEN IF(ARRAY_LENGTH(SPLIT(column_name, ".")) = 2, column_name, NULL)
      WHEN "metadata"
        THEN IF(ARRAY_LENGTH(SPLIT(column_name, ".")) = 3, column_name, NULL)
      WHEN "metrics"
        THEN IF(ARRAY_LENGTH(SPLIT(column_name, ".")) = 3, SPLIT(column_name, ".")[OFFSET(2)], NULL)
      ELSE column_name
    END AS metric_identifier,
    byte_size
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.column_size_v1`
  WHERE
    submission_date = @submission_date
    AND table_id = "metrics_v1"
),
combine_metrics_and_events AS (
  -- Event metrics from event_counts_glean_v2, concatenating category and name into the
  -- dot-joined "category.name" identifier (event_category/event_name are column values
  -- here, so the "." is preserved -- unlike the underscore-flattened metric columns
  -- above; see schema.yaml). The logical bytes collected for each event is:
  --   ((8 bytes of timestamp + category length + name length) * total events) + aggregated extras length
  -- event_extras_length is already aggregated upstream in v2, so it is added once and is
  -- NOT multiplied by total_events.
  SELECT
    submission_date,
    normalized_app_name,
    CONCAT(event_category, ".", event_name) AS metric_identifier,
    SUM(
      (
        (8 + BYTE_LENGTH(event_category) + BYTE_LENGTH(event_name)) * total_events
      ) + event_extras_length
    ) AS logical_bytes
  FROM
    `moz-fx-data-shared-prod.monitoring_derived.event_counts_glean_v2`
  WHERE
    submission_date = @submission_date
    AND normalized_app_name IN ("Firefox for Desktop", "Firefox for Android", "Firefox for iOS")
  GROUP BY
    submission_date,
    normalized_app_name,
    metric_identifier
  UNION ALL
  SELECT
    submission_date,
    normalized_app_name,
    metric_identifier,
    SUM(byte_size) AS logical_bytes
  FROM
    metrics_ping_metrics
  WHERE
    metric_identifier IS NOT NULL
    AND normalized_app_name IS NOT NULL
  GROUP BY
    submission_date,
    normalized_app_name,
    metric_identifier
)
SELECT
  submission_date,
  normalized_app_name,
  metric_identifier,
  SUM(logical_bytes) AS daily_logical_bytes
FROM
  combine_metrics_and_events
GROUP BY
  submission_date,
  normalized_app_name,
  metric_identifier
