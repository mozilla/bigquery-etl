#fail
{{ is_unique(["platform", "client_id"], "submission_date = @submission_date") }}

#fail
{{ not_null(["platform", "client_id"], "submission_date = @submission_date") }}

#fail
-- Every SAP search in the source must land here, in sap or, if excluded, in sap_excluded.
WITH kpi AS (
  SELECT
    SUM(sap) AS sap,
    SUM(sap_excluded) AS sap_excluded
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
    AND platform = 'desktop'
),
source AS (
  SELECT
    SUM(IF(sap_exclusion_reason IS NULL, sap_counts_total, 0)) AS sap,
    SUM(IF(sap_exclusion_reason IS NOT NULL, sap_counts_total, 0)) AS sap_excluded
  FROM
    `moz-fx-data-shared-prod.search.search_clients_daily_glean`
  WHERE
    submission_date = @submission_date
)
SELECT
  IF(
    kpi.sap IS DISTINCT FROM source.sap
    OR kpi.sap_excluded IS DISTINCT FROM source.sap_excluded,
    ERROR(
      FORMAT(
        "Desktop sap, sap_excluded are %d, %d here but %d, %d in search_clients_daily_glean",
        kpi.sap,
        kpi.sap_excluded,
        source.sap,
        source.sap_excluded
      )
    ),
    NULL
  )
FROM
  kpi,
  source;

#fail
-- Every Suggest and AI chat click in urlbar_clients_daily_v1 must land here: every urlbar
-- client is kept, so the totals match exactly.
WITH kpi AS (
  SELECT
    SUM(suggest_sponsored) AS suggest_sponsored,
    SUM(suggest_nonsponsored) AS suggest_nonsponsored,
    SUM(ai_chat) AS ai_chat
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
    AND platform = 'desktop'
),
component_map AS (
  SELECT
    result_type,
    component
  FROM
    `moz-fx-data-shared-prod.search_derived.kpi_component_map_v1`
  WHERE
    platform = 'desktop'
    AND effective_from <= @submission_date
    AND (effective_to IS NULL OR effective_to > @submission_date)
),
source AS (
  SELECT
    COALESCE(SUM(IF(m.component = 'suggest_sponsored', r.clicks, 0)), 0) AS suggest_sponsored,
    COALESCE(SUM(IF(m.component = 'suggest_nonsponsored', r.clicks, 0)), 0) AS suggest_nonsponsored,
    COALESCE(SUM(IF(m.component = 'ai_chat', r.clicks, 0)), 0) AS ai_chat
  FROM
    `moz-fx-data-shared-prod.search_derived.urlbar_clients_daily_v1` AS u
  CROSS JOIN
    UNNEST(u.results) AS r
  JOIN
    component_map AS m
    USING (result_type)
  WHERE
    u.submission_date = @submission_date
)
SELECT
  IF(
    kpi.suggest_sponsored IS DISTINCT FROM source.suggest_sponsored
    OR kpi.suggest_nonsponsored IS DISTINCT FROM source.suggest_nonsponsored
    OR kpi.ai_chat IS DISTINCT FROM source.ai_chat,
    ERROR(
      FORMAT(
        "Desktop Suggest clicks (sponsored, nonsponsored, ai_chat) are (%d, %d, %d) here but (%d, %d, %d) in urlbar_clients_daily_v1",
        kpi.suggest_sponsored,
        kpi.suggest_nonsponsored,
        kpi.ai_chat,
        source.suggest_sponsored,
        source.suggest_nonsponsored,
        source.ai_chat
      )
    ),
    NULL
  )
FROM
  kpi,
  source;

#fail
-- Mobile sap must not exceed mobile_search_aggregates for Firefox Android and Firefox iOS,
-- less the searches of clients the query leaves out for a deletion request, and each
-- platform must be in both. The aggregate sets the scope, so an app or version filter that
-- lets in too much fails here. The aggregate drops NULL-engine rows, which hold 0 searches.
-- Shredder only lowers this table's sap (it removes rows from mobile_search_clients_daily_v2
-- but not from the aggregate, which has no client ID), so this side never fails because of
-- Shredder.
WITH kpi AS (
  SELECT
    platform,
    SUM(sap) AS sap
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
    AND platform IN ('android', 'ios')
  GROUP BY
    platform
),
AGGREGATE
  AS (
    SELECT
      IF(os = 'iOS', 'ios', 'android') AS platform,
      SUM(search_count) AS sap
    FROM
      `moz-fx-data-shared-prod.search.mobile_search_aggregates`
    WHERE
      submission_date = @submission_date
      AND (
        (app_name IN ('Fenix', 'Firefox Preview') AND os = 'Android')
        OR (app_name = 'Fennec' AND os = 'iOS')
      )
    GROUP BY
      platform
  ),
  deleted AS (
    SELECT
      'android' AS platform,
      client_info.client_id
    FROM
      `moz-fx-data-shared-prod.fenix.deletion_request`
    WHERE
      DATE(submission_timestamp) >= '2019-01-01'
    UNION DISTINCT
    SELECT
      'ios' AS platform,
      client_info.client_id
    FROM
      `moz-fx-data-shared-prod.firefox_ios.deletion_request`
    WHERE
      DATE(submission_timestamp) >= '2019-01-01'
  ),
  deleted_sap AS (
    SELECT
      IF(s.os = 'iOS', 'ios', 'android') AS platform,
      SUM(s.search_count) AS sap
    FROM
      `moz-fx-data-shared-prod.search_derived.mobile_search_clients_daily_v2` AS s
    JOIN
      deleted AS d
      ON d.platform = IF(s.os = 'iOS', 'ios', 'android')
      AND d.client_id = s.client_id
    WHERE
      s.submission_date = @submission_date
      AND (
        (s.normalized_app_name = 'Fenix' AND s.os = 'Android')
        OR (
          s.normalized_app_name = 'Fennec'
          AND s.os = 'iOS'
          AND mozfun.norm.truncate_version(s.app_version, 'major') >= 28
        )
      )
    GROUP BY
      1
  )
SELECT
  IF(
    COUNT(*) > 0,
    ERROR(
      CONCAT(
        'Mobile sap is above mobile_search_aggregates less deleted clients, or a platform is missing, for: ',
        STRING_AGG(platform, ', ' ORDER BY platform)
      )
    ),
    NULL
  )
FROM
  kpi
FULL OUTER JOIN
AGGREGATE
  AS a
  USING (platform)
LEFT JOIN
  deleted_sap AS d
  USING (platform)
WHERE
  kpi.sap IS NULL
  OR a.sap IS NULL
  OR kpi.sap > a.sap - COALESCE(d.sap, 0);

#warn
-- Mobile sap below mobile_search_aggregates less deleted clients' searches. On a date that
-- Shredder has not processed yet, such as a nightly run, it means the query dropped clients.
-- On a rerun or backfill of a date Shredder has already cleaned, it is expected: Shredder has
-- removed deleted clients' rows from mobile_search_clients_daily_v2, and the aggregate still
-- counts their searches.
WITH kpi AS (
  SELECT
    platform,
    SUM(sap) AS sap
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
    AND platform IN ('android', 'ios')
  GROUP BY
    platform
),
AGGREGATE
  AS (
    SELECT
      IF(os = 'iOS', 'ios', 'android') AS platform,
      SUM(search_count) AS sap
    FROM
      `moz-fx-data-shared-prod.search.mobile_search_aggregates`
    WHERE
      submission_date = @submission_date
      AND (
        (app_name IN ('Fenix', 'Firefox Preview') AND os = 'Android')
        OR (app_name = 'Fennec' AND os = 'iOS')
      )
    GROUP BY
      platform
  ),
  deleted AS (
    SELECT
      'android' AS platform,
      client_info.client_id
    FROM
      `moz-fx-data-shared-prod.fenix.deletion_request`
    WHERE
      DATE(submission_timestamp) >= '2019-01-01'
    UNION DISTINCT
    SELECT
      'ios' AS platform,
      client_info.client_id
    FROM
      `moz-fx-data-shared-prod.firefox_ios.deletion_request`
    WHERE
      DATE(submission_timestamp) >= '2019-01-01'
  ),
  deleted_sap AS (
    SELECT
      IF(s.os = 'iOS', 'ios', 'android') AS platform,
      SUM(s.search_count) AS sap
    FROM
      `moz-fx-data-shared-prod.search_derived.mobile_search_clients_daily_v2` AS s
    JOIN
      deleted AS d
      ON d.platform = IF(s.os = 'iOS', 'ios', 'android')
      AND d.client_id = s.client_id
    WHERE
      s.submission_date = @submission_date
      AND (
        (s.normalized_app_name = 'Fenix' AND s.os = 'Android')
        OR (
          s.normalized_app_name = 'Fennec'
          AND s.os = 'iOS'
          AND mozfun.norm.truncate_version(s.app_version, 'major') >= 28
        )
      )
    GROUP BY
      1
  )
SELECT
  IF(
    COUNT(*) > 0,
    ERROR(
      CONCAT(
        'Mobile sap is below mobile_search_aggregates less deleted clients for: ',
        STRING_AGG(platform, ', ' ORDER BY platform)
      )
    ),
    NULL
  )
FROM
  kpi
FULL OUTER JOIN
AGGREGATE
  AS a
  USING (platform)
LEFT JOIN
  deleted_sap AS d
  USING (platform)
WHERE
  kpi.sap < a.sap - COALESCE(d.sap, 0);

#fail
-- Every counted mobile Suggest click in events_stream must land here, except those of clients
-- the query leaves out for a deletion request.
WITH kpi AS (
  SELECT
    platform,
    SUM(suggest_sponsored) AS suggest_sponsored,
    SUM(suggest_nonsponsored) AS suggest_nonsponsored
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
    AND platform IN ('android', 'ios')
  GROUP BY
    platform
),
component_map AS (
  SELECT
    platform,
    result_type,
    component
  FROM
    `moz-fx-data-shared-prod.search_derived.kpi_component_map_v1`
  WHERE
    platform IN ('android', 'ios')
    AND effective_from <= @submission_date
    AND (effective_to IS NULL OR effective_to > @submission_date)
),
deleted AS (
  SELECT
    'android' AS platform,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.fenix.deletion_request`
  WHERE
    DATE(submission_timestamp) >= '2019-01-01'
  UNION DISTINCT
  SELECT
    'ios' AS platform,
    client_info.client_id
  FROM
    `moz-fx-data-shared-prod.firefox_ios.deletion_request`
  WHERE
    DATE(submission_timestamp) >= '2019-01-01'
),
events AS (
  SELECT
    'android' AS platform,
    client_id,
    COALESCE(
      JSON_VALUE(event_extra, '$.provider'),
      JSON_VALUE(event_extra, '$.card_type')
    ) AS result_type
  FROM
    `moz-fx-data-shared-prod.fenix.events_stream`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_category = 'awesomebar'
    AND event_name IN (
      'sponsored_suggestion_clicked',
      'non_sponsored_suggestion_clicked',
      'optimized_suggestion_card_clicked'
    )
  UNION ALL
  SELECT
    'ios' AS platform,
    client_id,
    JSON_VALUE(event_extra, '$.type') AS result_type
  FROM
    `moz-fx-data-shared-prod.firefox_ios.events_stream`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_category = 'awesomebar'
    AND event_name = 'search_result_tap'
),
source AS (
  SELECT
    platform,
    COUNTIF(m.component = 'suggest_sponsored') AS suggest_sponsored,
    COUNTIF(m.component = 'suggest_nonsponsored') AS suggest_nonsponsored
  FROM
    events
  JOIN
    component_map AS m
    USING (platform, result_type)
  LEFT JOIN
    deleted
    USING (platform, client_id)
  WHERE
    deleted.client_id IS NULL
  GROUP BY
    platform
)
SELECT
  IF(
    COUNT(*) > 0,
    ERROR(
      CONCAT(
        'Mobile Suggest clicks differ from events_stream for: ',
        STRING_AGG(platform, ', ' ORDER BY platform)
      )
    ),
    NULL
  )
FROM
  kpi
FULL OUTER JOIN
  source
  USING (platform)
WHERE
  kpi.suggest_sponsored IS DISTINCT FROM source.suggest_sponsored
  OR kpi.suggest_nonsponsored IS DISTINCT FROM source.suggest_nonsponsored;

#warn
-- Early warning: a result type shown but not yet clicked with no map row fails the run on its
-- first release-channel click. action_* types are excluded by the query; the next check lists
-- clicked ones. A name cut short by the 500-byte limit on the results field is a prefix of a
-- real type, so it isn't a new type.
WITH component_map AS (
  SELECT
    result_type
  FROM
    `moz-fx-data-shared-prod.search_derived.kpi_component_map_v1`
  WHERE
    platform = 'desktop'
),
shown AS (
  SELECT DISTINCT
    r.result_type
  FROM
    `moz-fx-data-shared-prod.search_derived.urlbar_clients_daily_v1` AS u
  CROSS JOIN
    UNNEST(u.results) AS r
  WHERE
    u.submission_date = @submission_date
),
new_types AS (
  SELECT
    result_type
  FROM
    shown AS s
  WHERE
    NOT STARTS_WITH(result_type, 'action_')
    AND NOT EXISTS (
      SELECT
        1
      FROM
        component_map AS m
      WHERE
        STARTS_WITH(m.result_type, s.result_type)
    )
)
SELECT
  IF(
    COUNT(*) > 0,
    ERROR(
      CONCAT(
        'Result types missing from kpi_component_map_v1: ',
        STRING_AGG(result_type, ', ' ORDER BY result_type)
      )
    ),
    NULL
  )
FROM
  new_types;

#warn
-- The query excludes a clicked type with no map row, without failing, when it's in a family
-- that never counts (action_, autofill_, intervention_, tip_, restrict_keyword_) or clicked
-- only on pre-release channels. This lists them so each gets a row, in case one should count.
WITH component_map AS (
  SELECT
    result_type
  FROM
    `moz-fx-data-shared-prod.search_derived.kpi_component_map_v1`
  WHERE
    platform = 'desktop'
),
unmapped_clicks AS (
  SELECT DISTINCT
    r.result_type
  FROM
    `moz-fx-data-shared-prod.search_derived.urlbar_clients_daily_v1` AS u
  CROSS JOIN
    UNNEST(u.results) AS r
  LEFT JOIN
    component_map AS m
    USING (result_type)
  WHERE
    u.submission_date = @submission_date
    AND r.clicks > 0
    AND m.result_type IS NULL
)
SELECT
  IF(
    COUNT(*) > 0,
    ERROR(
      CONCAT(
        'Clicked result types with no kpi_component_map_v1 row, excluded from the KPI: ',
        STRING_AGG(result_type, ', ' ORDER BY result_type)
      )
    ),
    NULL
  )
FROM
  unmapped_clicks;

#warn
-- Early warning for mobile: a Suggest type shown but not yet clicked with no map row fails the
-- run on its first click. The impression events carry the same keys as the clicks.
WITH component_map AS (
  SELECT
    platform,
    result_type
  FROM
    `moz-fx-data-shared-prod.search_derived.kpi_component_map_v1`
  WHERE
    platform IN ('android', 'ios')
),
shown AS (
  SELECT DISTINCT
    'android' AS platform,
    COALESCE(
      JSON_VALUE(event_extra, '$.provider'),
      JSON_VALUE(event_extra, '$.card_type')
    ) AS result_type
  FROM
    `moz-fx-data-shared-prod.fenix.events_stream`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_category = 'awesomebar'
    AND event_name IN (
      'sponsored_suggestion_impressed',
      'non_sponsored_suggestion_impressed',
      'optimized_suggestion_card_displayed'
    )
  UNION DISTINCT
  SELECT DISTINCT
    'ios' AS platform,
    JSON_VALUE(event_extra, '$.type') AS result_type
  FROM
    `moz-fx-data-shared-prod.firefox_ios.events_stream`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND event_category = 'awesomebar'
    AND event_name = 'search_result_impression'
)
SELECT
  IF(
    COUNT(*) > 0,
    ERROR(
      CONCAT(
        'Mobile result types shown with no kpi_component_map_v1 row: ',
        STRING_AGG(
          CONCAT(platform, ':', COALESCE(result_type, 'NULL')),
          ', '
          ORDER BY
            platform,
            result_type
        )
      )
    ),
    NULL
  )
FROM
  shown
LEFT JOIN
  component_map AS m
  USING (platform, result_type)
WHERE
  m.result_type IS NULL;
