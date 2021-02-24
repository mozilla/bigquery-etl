CREATE TEMPORARY FUNCTION labeled_counter(
  `values` ARRAY<STRUCT<key STRING, value INT64>>,
  labels ARRAY<STRING>
) AS (
  (
    WITH summed AS (
      SELECT
        IF(a.key IN (SELECT * FROM UNNEST(labels)), a.key, "__unknown__") AS k,
        SUM(a.value) AS v
      FROM
        UNNEST(`values`) AS a
      GROUP BY
        a.key
    )
    SELECT
      ARRAY_AGG(STRUCT<key STRING, value INT64>(k, v))
    FROM
      summed
  )
);

WITH extracted_event AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived.legacy_mobile_event_counts_v1`
  WHERE
    submission_date = @submission_date
),
extracted_core AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.org_mozilla_ios_firefox_derived.legacy_mobile_core_v1`
  WHERE
    submission_date = @submission_date
),
labeled AS (
  SELECT
    client_id,
    submission_date,
    ARRAY_AGG(
      IF(object = "bookmark" AND method = "open", (method, value), NULL) IGNORE NULLS
    ) AS bookmarks_open,
    ARRAY_AGG(
      IF(object = "bookmark" AND method = "add", (method, value), NULL) IGNORE NULLS
    ) AS bookmarks_add,
    ARRAY_AGG(
      IF(object = "bookmark" AND method = "delete", (method, value), NULL) IGNORE NULLS
    ) AS bookmarks_delete,
    ARRAY_AGG(
      IF(
        object = "bookmarks-panel",
        CASE
        WHEN
          string_value = "app-menu"
        THEN
          ("app-menu", value)
        WHEN
          string_value LIKE "home-panel%"
        THEN
          ("home-panel", value)
        ELSE
          NULL
        END
        ,
        NULL
      ) IGNORE NULLS
    ) AS bookmarks_view_list,
    ARRAY_AGG(
      IF(object = "reading-list" AND method = "add", (method, value), NULL) IGNORE NULLS
    ) AS reading_list_add,
    ARRAY_AGG(
      IF(object = "reading-list" AND method = "delete", (method, value), NULL) IGNORE NULLS
    ) AS reading_list_delete,
    -- tabs
    ARRAY_AGG(
      IF(object = "tab" AND method = "add", (method, value), NULL) IGNORE NULLS
    ) AS tabs_open,
    ARRAY_AGG(
      IF(object = "tab" AND method = "close", (method, value), NULL) IGNORE NULLS
    ) AS tabs_close
  FROM
    extracted_event
  GROUP BY
    client_id,
    submission_date
),
aggregated AS (
  SELECT
    client_id,
    submission_date,
    -- background can be calculated too
    SUM(
      IF(object = "app" AND method = "foreground", value, 0)
    ) AS counter_glean_validation_foreground_count,
    ANY_VALUE(
      labeled_counter(bookmarks_open, ["awesomebar-results", "bookmarks-panel"])
    ) AS labeled_counter_bookmarks_open,
    -- NOTE: should share-menu actually be context-menu?
    ANY_VALUE(
      labeled_counter(bookmarks_add, ["page-action-menu", "share-menu", "activity-stream"])
    ) AS labeled_counter_bookmarks_add,
    ANY_VALUE(
      labeled_counter(bookmarks_delete, ["page-action-menu", "activity-stream", "bookmarks-panel"])
    ) AS labeled_counter_bookmarks_delete,
    ANY_VALUE(
      labeled_counter(bookmarks_view_list, ["home-panel", "app-menu"])
    ) AS labeled_counter_bookmarks_view_list,
    ANY_VALUE(
      labeled_counter(tabs_open, ["normal-tab", "private-tab"])
    ) AS labeled_counter_tabs_open,
    ANY_VALUE(
      labeled_counter(tabs_close, ["normal-tab", "private-tab"])
    ) AS labeled_counter_tabs_close,
    SUM(IF(object = "reader-mode-open-button", value, 0)) AS counter_reader_mode_open,
    SUM(IF(object = "reader-mode-close-button", value, 0)) AS counter_reader_mode_close,
    ANY_VALUE(
      labeled_counter(
        reading_list_add,
        ["reader-mode-toolbar", "share-extension", "page-action-menu"]
      )
    ) AS labeled_counter_reading_list_add,
    ANY_VALUE(
      labeled_counter(reading_list_delete, ["reader-mode-toolbar", "reading-list-panel"])
    ) AS labeled_counter_reading_list_delete,
    SUM(
      IF(object = "reading-list-item" AND method = "open", value, 0)
    ) AS counter_reading_list_open,
    SUM(
      IF(object = "reading-list-item" AND method = "mark-as-read", value, 0)
    ) AS counter_reading_list_mark_read,
    SUM(
      IF(object = "reading-list-item" AND method = "mark-as-unread", value, 0)
    ) AS counter_reading_list_mark_unread,
    SUM(IF(object LIKE "qr-code%" AND method = "scan", value, 0)) AS counter_qr_code_scanned,
  FROM
    extracted_event
  JOIN
    labeled
  USING
    (client_id, submission_date)
  GROUP BY
    client_id,
    submission_date
)
SELECT
  submission_timestamp,
  (SELECT AS STRUCT metadata.* EXCEPT (uri)) AS metadata,
  normalized_app_name,
  normalized_channel,
  normalized_country_code,
  normalized_os,
  normalized_os_version,
  STRUCT(
    client_id,
    CAST(NULL AS string) AS android_sdk_version,
    metadata.uri.app_build_id AS app_build,
    metadata.uri.app_update_channel AS app_channel,
    metadata.uri.app_version AS app_display_version,
    arch AS architecture,
    "Apple" AS device_manufacturer,
    device AS device_model,
    locale,
    os,
    osversion AS os_version,
    CAST(NULL AS string) AS telemetry_sdk_build
  ) AS client_info,
  STRUCT(
    STRUCT(
      string_search_default_engine AS search_default_engine,
      string_preferences_mail_client AS preferences_mail_client,
      string_preferences_new_tab_experience AS preferences_new_tab_experience
    ) AS string,
    STRUCT(
      counter_tabs_cumulative_count AS tabs_cumulative_count,
      counter_glean_validation_foreground_count AS glean_validation_foreground_count,
      counter_reader_mode_open AS reader_mode_open,
      counter_reader_mode_close AS reader_mode_close,
      counter_reading_list_open AS reading_list_open,
      counter_reading_list_mark_read AS reading_list_mark_read,
      counter_reading_list_mark_unread AS reading_list_mark_unread,
      counter_qr_code_scanned AS qr_code_scanned
    ) AS counter,
    STRUCT(
      labeled_counter_search_counts AS search_counts,
      labeled_counter_bookmarks_view_list AS bookmarks_view_list,
      labeled_counter_bookmarks_open AS bookmarks_open,
      labeled_counter_bookmarks_add AS bookmarks_add,
      labeled_counter_bookmarks_delete AS bookmarks_delete,
      labeled_counter_tabs_open AS tabs_open,
      labeled_counter_tabs_close AS tabs_close,
      labeled_counter_reading_list_add AS reading_list_add,
      labeled_counter_reading_list_delete AS reading_list_delete
    ) AS labeled_counter
  ) AS metrics
FROM
  aggregated
JOIN
  extracted_event
USING
  (client_id, submission_date)
JOIN
  extracted_core
USING
  (client_id, submission_date)
