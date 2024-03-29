CREATE TEMP FUNCTION is_ad_component(component STRING) AS (
  -- tag components containing monetizable ads
  COALESCE(
    component IN ('ad_carousel', 'ad_image_row', 'ad_link', 'ad_sidebar', 'ad_sitelink'),
    FALSE
  )
);

CREATE TEMP FUNCTION ad_blocker_inferred(num_loaded INT, num_blocked INT) AS (
  -- ad blocker is inferred to be in use if all ads are blocked
  num_loaded > 0
  AND num_blocked = num_loaded
);

CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.serp_events`
AS
WITH annotated AS (
  SELECT
    * EXCEPT (engagements, component_impressions),
    -- tag engagement counts for ad components
    ARRAY(
      SELECT AS STRUCT
        c.*,
        is_ad_component(c.component) AS is_ad_component
      FROM
        UNNEST(engagements) AS c
    ) AS engagements,
    -- tag impression counts for ad components
    -- tag components with all elements blocked
    ARRAY(
      SELECT AS STRUCT
        c.*,
        is_ad_component(c.component) AS is_ad_component,
        ad_blocker_inferred(c.num_elements_loaded, num_elements_blocked) AS blocker_inferred
      FROM
        UNNEST(component_impressions) AS c
    ) AS component_impressions,
  FROM
    `{{ project_id }}.{{ app_name }}_derived.serp_events_v1`
),
with_counts AS (
  -- add SERP-impression-level counts of ad impresions and clicks
  SELECT
    *,
    (
      SELECT AS STRUCT
        COALESCE(SUM(a.num_elements_loaded), 0) AS num_ads_loaded,
        COALESCE(SUM(a.num_elements_visible), 0) AS num_ads_visible,
        COALESCE(SUM(a.num_elements_blocked), 0) AS num_ads_blocked,
        COALESCE(SUM(a.num_elements_notshowing), 0) AS num_ads_notshowing,
        -- an ad blocker is inferred to be in use if all ads are blocked in at least one ad component
        -- FALSE if no ads are loaded because we cannot infer
        COALESCE(LOGICAL_OR(is_ad_component AND blocker_inferred), FALSE) AS ad_blocker_inferred
      FROM
        UNNEST(component_impressions) AS a
      WHERE
        a.is_ad_component
    ).*,
    (
      SELECT AS STRUCT
        -- monetizable ad clicks
        COALESCE(
          SUM(IF(e.is_ad_component AND e.action = 'clicked', num_engagements, 0)),
          0
        ) AS num_ad_clicks,
        -- clicks on other components
        COALESCE(
          SUM(IF(NOT e.is_ad_component AND e.action = 'clicked', num_engagements, 0)),
          0
        ) AS num_non_ad_clicks,
        -- other types of engagement, eg. 'expanded'
        COALESCE(SUM(IF(e.action != 'clicked', num_engagements, 0)), 0) AS num_other_engagements
      FROM
        UNNEST(engagements) AS e
    ).*
  FROM
    annotated
)
SELECT
  * EXCEPT (num_ad_clicks),
  -- clicks must refer to visible ads (impressions)
  IF(num_ads_visible > 0, num_ad_clicks, 0) AS num_ad_clicks
FROM
  with_counts
