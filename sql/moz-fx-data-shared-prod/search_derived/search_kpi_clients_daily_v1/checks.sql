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
-- Every mobile SAP search in the source must land here: a client with SAP is always kept.
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
source AS (
  SELECT
    IF(os = 'iOS', 'ios', 'android') AS platform,
    SUM(search_count) AS sap
  FROM
    `moz-fx-data-shared-prod.search_derived.mobile_search_clients_daily_v2`
  WHERE
    submission_date = @submission_date
    AND (
      (normalized_app_name = 'Fenix' AND os = 'Android')
      OR (normalized_app_name = 'Fennec' AND os = 'iOS')
    )
  GROUP BY
    platform
)
SELECT
  IF(
    COUNT(*) > 0,
    ERROR(
      CONCAT(
        'Mobile sap differs from mobile_search_clients_daily_v2 for: ',
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
  kpi.sap IS DISTINCT FROM source.sap;

#fail
-- Every counted mobile Suggest click in events_stream must land here.
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
events AS (
  SELECT
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
      'sponsored_suggestion_clicked',
      'non_sponsored_suggestion_clicked',
      'optimized_suggestion_card_clicked'
    )
  UNION ALL
  SELECT
    'ios' AS platform,
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
-- first click. action_* types are excluded by the query. A name cut short by the 500-byte limit
-- on the results field is a prefix of a real type, so it isn't a new type.
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
-- action_* types are excluded without a map row, so a new one can't fail the run. This lists
-- clicked ones with no row, in case one should count.
WITH component_map AS (
  SELECT
    result_type
  FROM
    `moz-fx-data-shared-prod.search_derived.kpi_component_map_v1`
  WHERE
    platform = 'desktop'
),
new_actions AS (
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
    AND STARTS_WITH(r.result_type, 'action_')
    AND r.clicks > 0
    AND m.result_type IS NULL
)
SELECT
  IF(
    COUNT(*) > 0,
    ERROR(
      CONCAT(
        'Clicked action_* types with no kpi_component_map_v1 row: ',
        STRING_AGG(result_type, ', ' ORDER BY result_type)
      )
    ),
    NULL
  )
FROM
  new_actions;
