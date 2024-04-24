CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.serp_events`
AS
WITH expanded AS (
  SELECT
    * EXCEPT (engagements, component_impressions),
    (
      -- use value table to compute multiple columns from unnest(engagements)
      SELECT AS STRUCT
        -- inject ad component indicator into engagements array
        ARRAY_AGG(STRUCT(component, action, num_engagements, is_ad_component)) AS engagements,
        -- monetizable ad clicks
        COALESCE(
          SUM(IF(is_ad_component AND action = 'clicked', num_engagements, 0)),
          0
        ) AS num_ad_clicks,
        -- non-ad link clicks
        COALESCE(
          SUM(IF(component = 'non_ads_link' AND action = 'clicked', num_engagements, 0)),
          0
        ) AS num_non_ad_link_clicks,
        -- other types of engagement
        COALESCE(
          SUM(
            IF(
              action != 'clicked'
              OR (action = 'clicked' AND NOT is_ad_component AND component != 'non_ads_link'),
              num_engagements,
              0
            )
          ),
          0
        ) AS num_other_engagements
      FROM
        (SELECT e.*, is_ad_component(e.component) AS is_ad_component FROM UNNEST(engagements) AS e)
    ).*,
    (
      -- use value table to compute multiple columns from unnest(component_impressions)
      SELECT AS STRUCT
        -- inject ad component and blocking indicators into components array
        ARRAY_AGG(
          STRUCT(
            component,
            num_elements_loaded,
            num_elements_visible,
            num_elements_blocked,
            num_elements_notshowing,
            is_ad_component,
            blocker_inferred
          )
        ) AS component_impressions,
        COALESCE(SUM(IF(is_ad_component, num_elements_loaded, 0)), 0) AS num_ads_loaded,
        COALESCE(SUM(IF(is_ad_component, num_elements_visible, 0)), 0) AS num_ads_visible,
        COALESCE(SUM(IF(is_ad_component, num_elements_blocked, 0)), 0) AS num_ads_blocked,
        COALESCE(SUM(IF(is_ad_component, num_elements_notshowing, 0)), 0) AS num_ads_notshowing,
        -- an ad blocker is inferred to be in use if all ads are blocked in at least one ad component
        -- FALSE if no ads are loaded because we cannot infer
        COALESCE(LOGICAL_OR(is_ad_component AND blocker_inferred), FALSE) AS ad_blocker_inferred
      FROM
        (
          SELECT
            c.*,
            is_ad_component(c.component) AS is_ad_component,
            ad_blocker_inferred(c.num_elements_loaded, c.num_elements_blocked) AS blocker_inferred
          FROM
            UNNEST(component_impressions) AS c
        )
    ).*
  FROM
    `{{ project_id }}.{{ app_name }}_derived.serp_events_v2`
),
SELECT
  * EXCEPT (num_ad_clicks),
  -- clicks must refer to visible ads (impressions)
  IF(num_ads_visible > 0, num_ad_clicks, 0) AS num_ad_clicks
FROM
  expanded
