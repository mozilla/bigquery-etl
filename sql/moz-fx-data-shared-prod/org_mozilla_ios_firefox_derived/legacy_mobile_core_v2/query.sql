CREATE TEMP FUNCTION rename_search(value STRING) AS (
    -- in glean, this is implemented as search-engine-name.source
  CONCAT(SPLIT(value, ".")[OFFSET(1)], ".", SPLIT(value, ".")[OFFSET(0)])
);

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
        k
    )
    SELECT
      ARRAY_AGG(STRUCT<key STRING, value INT64>(k, v))
    FROM
      summed
  )
);

WITH unioned AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v2`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v3`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v4`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v5`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v6`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v7`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v8`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v9`
  UNION ALL
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.core_v10`
),
extracted AS (
  SELECT
    * EXCEPT (client_id, document_id),
    LOWER(client_id) AS client_id,
    LOWER(document_id) AS document_id,
    DATE(submission_timestamp) AS submission_date,
  FROM
    unioned
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND normalized_os = 'iOS'
    AND normalized_app_name = 'Fennec'
),
extracted_meta AS (
  SELECT
    * EXCEPT (
      searches,
      default_search,
      default_mail_client,
      default_new_tab_experience,
      open_tab_count
    )
  FROM
    extracted
),
meta_ranked AS (
  SELECT
    t AS metadata,
    ROW_NUMBER() OVER (
      PARTITION BY
        client_id,
        submission_date
      ORDER BY
        submission_timestamp DESC
    ) AS _n
  FROM
    extracted_meta t
),
meta AS (
  SELECT
    metadata.*
  FROM
    meta_ranked
  WHERE
    _n = 1
),
searches AS (
  SELECT
    submission_date,
    client_id,
    rename_search(s.key) AS key,
    SUM(s.value) AS value
  FROM
    extracted,
    UNNEST(searches) s
  GROUP BY
    1,
    2,
    3
),
searches_grouped AS (
  SELECT
    submission_date,
    client_id,
    ARRAY_AGG(STRUCT(key, value)) AS labeled_counter_search_counts
  FROM
    searches
  GROUP BY
    1,
    2
),
rest_grouped AS (
  SELECT
    submission_date,
    client_id,
    mozfun.stats.mode_last(ARRAY_AGG(default_search)) AS string_search_default_engine,
    mozfun.stats.mode_last(ARRAY_AGG(default_mail_client)) AS string_preferences_mail_client,
    mozfun.stats.mode_last(
      ARRAY_AGG(default_new_tab_experience)
    ) AS string_preferences_new_tab_experience,
    -- NOTE: that we cannot recover the average count because we are summing this up into one ping. An
    -- average might be more desirable, but it doesn't quite match the metric name.
    SUM(open_tab_count) AS counter_tabs_cumulative_count
  FROM
    extracted
  GROUP BY
    1,
    2
)
SELECT
  submission_timestamp,
  document_id,
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
    STRUCT(counter_tabs_cumulative_count AS tabs_cumulative_count) AS counter,
    STRUCT(labeled_counter_search_counts AS search_counts) AS labeled_counter
  ) AS metrics
FROM
  searches_grouped
JOIN
  rest_grouped
  USING (submission_date, client_id)
JOIN
  meta
  USING (submission_date, client_id)
