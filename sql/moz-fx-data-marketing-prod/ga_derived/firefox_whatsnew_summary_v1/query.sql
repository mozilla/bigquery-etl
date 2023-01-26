WITH wnp_visits AS (
  SELECT
    date,
    visit_identifier,
    hit_number,
    bounces,
    first_interaction,
    trim(page_path_level1, '/') AS locale,
    page_level_2 AS version,
    country
  FROM
    `moz-fx-data-marketing-prod.ga_derived.www_site_hits_v1`
  WHERE
    date = '2021-01-01'
    AND page_name LIKE '%/firefox/%/whatsnew%'
    AND hit_type = 'PAGE'
),
bounce_flagging AS (
  SELECT
    *,
    CASE
    WHEN
      hit_number = first_interaction
      AND bounces = 1
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
  count(DISTINCT visit_identifier) AS visits,
  count(DISTINCT CASE WHEN bounce_flag = 1 THEN visit_identifier END) AS bounces
FROM
  bounce_flagging
GROUP BY
  1,
  2,
  3,
  4;
