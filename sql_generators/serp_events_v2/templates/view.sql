CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ app_name }}.serp_events`
AS
WITH with_ads_arrays AS (
  -- prepare arrays for ad impressions and engagements with the same structure
  -- to be combined in the next step
  SELECT
    *,
    ARRAY(
      SELECT AS STRUCT
        component,
        0 AS num_elements_loaded,
        0 AS num_elements_visible,
        0 AS num_elements_blocked,
        0 AS num_elements_notshowing,
        FALSE AS blocker_inferred,
        IF(action = 'clicked', num_engagements, 0) AS num_clicks,
        IF(action != 'clicked', num_engagements, 0) AS num_other_engagements
      FROM
        UNNEST(engagements)
      WHERE
        mozfun.serp_events.is_ad_component(component)
    ) AS _ad_engagements,
    ARRAY(
      SELECT AS STRUCT
        component,
        num_elements_loaded,
        num_elements_visible,
        num_elements_blocked,
        num_elements_notshowing,
        mozfun.serp_events.ad_blocker_inferred(
          num_elements_loaded,
          num_elements_blocked
        ) AS blocker_inferred,
        0 AS num_clicks,
        0 AS num_other_engagements
      FROM
        UNNEST(component_impressions)
      WHERE
        mozfun.serp_events.is_ad_component(component)
    ) AS _ad_impressions,
  FROM
    `{{ project_id }}.{{ app_name }}_derived.serp_events_v2`
)
SELECT
  * EXCEPT (engagements, component_impressions, _ad_engagements, _ad_impressions),
  -- array for ad components "joining" impressions and engagements
  -- aggregate to get 1 entry per ad component
  ARRAY(
    SELECT AS STRUCT
      component,
      SUM(num_elements_loaded) AS num_loaded,
      SUM(num_elements_visible) AS num_visible,
      SUM(num_elements_blocked) AS num_blocked,
      SUM(num_elements_notshowing) AS num_notshowing,
      SUM(num_clicks) AS num_clicks,
      SUM(num_other_engagements) AS num_other_engagements,
      LOGICAL_OR(blocker_inferred) AS blocker_inferred
    FROM
      UNNEST(ARRAY_CONCAT(_ad_impressions, _ad_engagements))
    GROUP BY
      component
  ) AS ad_components,
  -- remaining engagements for non-ad components
  ARRAY(
    SELECT
      e
    FROM
      UNNEST(engagements) AS e
    WHERE
      NOT mozfun.serp_events.is_ad_component(e.component)
  ) AS non_ad_engagements,
  -- remaining impressions for non-ad components
  ARRAY(
    SELECT
      c
    FROM
      UNNEST(component_impressions) AS c
    WHERE
      NOT mozfun.serp_events.is_ad_component(c.component)
  ) AS non_ad_impressions,
  -- total ad clicks for the SERP impression
  (
    SELECT
      COALESCE(SUM(num_engagements), 0)
    FROM
      UNNEST(engagements)
    WHERE
      mozfun.serp_events.is_ad_component(component)
      AND action = 'clicked'
  ) AS num_ad_clicks,
  -- total non-ad link clicks for the SERP impression
  (
    SELECT
      COALESCE(SUM(num_engagements), 0)
    FROM
      UNNEST(engagements)
    WHERE
      component = 'non_ads_link'
      AND action = 'clicked'
  ) AS num_non_ad_link_clicks,
  -- total other engagements for the SERP impression
  (
    SELECT
      COALESCE(SUM(num_engagements), 0)
    FROM
      UNNEST(engagements)
    WHERE
      action != 'clicked'
      OR (
        action = 'clicked'
        AND NOT mozfun.serp_events.is_ad_component(component)
        AND component != 'non_ads_link'
      )
  ) AS num_other_engagements,
  -- total ads loaded for the SERP impression
  (
    SELECT
      COALESCE(SUM(num_elements_loaded), 0)
    FROM
      UNNEST(component_impressions)
    WHERE
      mozfun.serp_events.is_ad_component(component)
  ) AS num_ads_loaded,
  -- total ads visible for the SERP impression
  (
    SELECT
      COALESCE(SUM(num_elements_visible), 0)
    FROM
      UNNEST(component_impressions)
    WHERE
      mozfun.serp_events.is_ad_component(component)
  ) AS num_ads_visible,
  -- total ads blocked for the SERP impression
  (
    SELECT
      COALESCE(SUM(num_elements_blocked), 0)
    FROM
      UNNEST(component_impressions)
    WHERE
      mozfun.serp_events.is_ad_component(component)
  ) AS num_ads_blocked,
  -- total ads not showing for the SERP impression
  (
    SELECT
      COALESCE(SUM(num_elements_notshowing), 0)
    FROM
      UNNEST(component_impressions)
    WHERE
      mozfun.serp_events.is_ad_component(component)
  ) AS num_ads_notshowing,
  -- is ad blocker inferred?
  -- true if blocker is inferred for at least 1 component
  (
    SELECT
      COALESCE(
        LOGICAL_OR(
          mozfun.serp_events.ad_blocker_inferred(num_elements_loaded, num_elements_blocked)
        ),
        FALSE
      )
    FROM
      UNNEST(component_impressions)
    WHERE
      mozfun.serp_events.is_ad_component(component)
  ) AS ad_blocker_inferred,
FROM
  with_ads_arrays
