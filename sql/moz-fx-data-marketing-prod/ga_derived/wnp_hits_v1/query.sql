WITH wnp_visits AS (
  SELECT
    PARSE_DATE('%Y%m%d', date) AS date,
    CONCAT(CAST(fullVisitorId AS STRING), CAST(visitId AS STRING)) AS visit_identifier,
    hit.hitNumber,
    totals.bounces,
    MIN(IF(hit.isInteraction IS NOT NULL, hit.hitNumber, 0)) OVER (
      PARTITION BY
        fullVisitorId,
        visitStartTime
    ) AS first_interaction,
    SPLIT(split(hit.page.pagePath, '?')[offset(0)], '/') AS split_page_path,
    geoNetwork.country
  FROM
    `moz-fx-data-marketing-prod.65789850.ga_sessions_*`
  CROSS JOIN
    UNNEST(hits) AS hit
  WHERE
    _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', '2021-01-01')
    AND hit.page.pagePath LIKE '%whatsnew%'
    AND hit.type = 'PAGE'
),
bounce_flagging AS (
  SELECT
    * EXCEPT (split_page_path),
    split_page_path[safe_offset(1)] locale,
    split_page_path[safe_offset(3)] version,
    CASE
    WHEN
      hitNumber = first_interaction
    THEN
      1
    ELSE
      0
    END
    AS bounce_flag
  FROM
    wnp_visits
)
SELECT
  date,
  country,
  locale,
  version,
  count(DISTINCT visit_identifier) AS WNP_visits,
  count(DISTINCT CASE WHEN bounce_flag = 1 THEN visit_identifier END) AS bounces
FROM
  bounce_flagging
GROUP BY
  1,
  2,
  3,
  4
