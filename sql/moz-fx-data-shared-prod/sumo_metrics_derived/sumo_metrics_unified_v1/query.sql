-- Freshness Metrics (no product dimension)
SELECT
  event_date AS `date`,
  CAST(NULL AS STRING) AS product,
  CAST(NULL AS STRING) AS content_type,
  'freshness_metrics' AS source,
  -- Freshness-specific columns
  total_articles,
  articles_updated_within_6_months,
  outdated_articles,
  articles_0_3_months,
  articles_3_6_months,
  articles_6_12_months,
  articles_12_plus_months,
  freshness_percentage,
  freshness_tier,
  meets_75pct_target,
  meets_90pct_stretch_goal,
  -- GA4-specific columns (NULL)
  CAST(NULL AS INT64) AS sessions,
  CAST(NULL AS INT64) AS events,
  CAST(NULL AS FLOAT64) AS events_per_session,
  -- Kitsune-specific columns (NULL)
  CAST(NULL AS INT64) AS forum_questions_posted,
  -- Zendesk-specific columns (NULL)
  CAST(NULL AS INT64) AS zendesk_tickets_created
FROM
  `moz-fx-data-shared-prod.sumo_metrics_derived.freshness_metrics_base_v1`
UNION ALL
-- GA4 Engagement Sessions
SELECT
  event_date AS `date`,
  product,
  content_type,
  'ga4_engagement_sessions' AS source,
  -- Freshness-specific columns (NULL)
  CAST(NULL AS INT64) AS total_articles,
  CAST(NULL AS INT64) AS articles_updated_within_6_months,
  CAST(NULL AS INT64) AS outdated_articles,
  CAST(NULL AS INT64) AS articles_0_3_months,
  CAST(NULL AS INT64) AS articles_3_6_months,
  CAST(NULL AS INT64) AS articles_6_12_months,
  CAST(NULL AS INT64) AS articles_12_plus_months,
  CAST(NULL AS NUMERIC) AS freshness_percentage,
  CAST(NULL AS STRING) AS freshness_tier,
  CAST(NULL AS BOOL) AS meets_75pct_target,
  CAST(NULL AS BOOL) AS meets_90pct_stretch_goal,
  -- GA4-specific columns
  sessions,
  events,
  events_per_session,
  -- Kitsune-specific columns (NULL)
  CAST(NULL AS INT64) AS forum_questions_posted,
  -- Zendesk-specific columns (NULL)
  CAST(NULL AS INT64) AS zendesk_tickets_created
FROM
  `moz-fx-data-shared-prod.sumo_metrics_derived.ga4_engagement_sessions_daily_v1`
UNION ALL
-- Kitsune Forum Questions
SELECT
  creation_date AS `date`,
  product,
  CAST(NULL AS STRING) AS content_type,
  'kitsune_questions' AS source,
  -- Freshness-specific columns (NULL)
  CAST(NULL AS INT64) AS total_articles,
  CAST(NULL AS INT64) AS articles_updated_within_6_months,
  CAST(NULL AS INT64) AS outdated_articles,
  CAST(NULL AS INT64) AS articles_0_3_months,
  CAST(NULL AS INT64) AS articles_3_6_months,
  CAST(NULL AS INT64) AS articles_6_12_months,
  CAST(NULL AS INT64) AS articles_12_plus_months,
  CAST(NULL AS NUMERIC) AS freshness_percentage,
  CAST(NULL AS STRING) AS freshness_tier,
  CAST(NULL AS BOOL) AS meets_75pct_target,
  CAST(NULL AS BOOL) AS meets_90pct_stretch_goal,
  -- GA4-specific columns (NULL)
  CAST(NULL AS INT64) AS sessions,
  CAST(NULL AS INT64) AS events,
  CAST(NULL AS FLOAT64) AS events_per_session,
  -- Kitsune-specific columns
  forum_questions_posted,
  -- Zendesk-specific columns (NULL)
  CAST(NULL AS INT64) AS zendesk_tickets_created
FROM
  `moz-fx-data-shared-prod.sumo_metrics_derived.kitsune_questions_base_v1`
UNION ALL
-- Zendesk Tickets
SELECT
  DATE AS `date`,
  product,
  CAST(NULL AS STRING) AS content_type,
  'zendesk_tickets' AS source,
  -- Freshness-specific columns (NULL)
  CAST(NULL AS INT64) AS total_articles,
  CAST(NULL AS INT64) AS articles_updated_within_6_months,
  CAST(NULL AS INT64) AS outdated_articles,
  CAST(NULL AS INT64) AS articles_0_3_months,
  CAST(NULL AS INT64) AS articles_3_6_months,
  CAST(NULL AS INT64) AS articles_6_12_months,
  CAST(NULL AS INT64) AS articles_12_plus_months,
  CAST(NULL AS NUMERIC) AS freshness_percentage,
  CAST(NULL AS STRING) AS freshness_tier,
  CAST(NULL AS BOOL) AS meets_75pct_target,
  CAST(NULL AS BOOL) AS meets_90pct_stretch_goal,
  -- GA4-specific columns (NULL)
  CAST(NULL AS INT64) AS sessions,
  CAST(NULL AS INT64) AS events,
  CAST(NULL AS FLOAT64) AS events_per_session,
  -- Kitsune-specific columns (NULL)
  CAST(NULL AS INT64) AS forum_questions_posted,
  -- Zendesk-specific columns
  zendesk_tickets_created
FROM
  `moz-fx-data-shared-prod.sumo_metrics_derived.zendesk_tickets_base_v1`
