WITH wnp_visits AS (
  SELECT
    date,
    visit_identifier,
    hit_number,
    bounces,
    first_interaction,
    TRIM(page_path_level1, '/') AS locale,
    page_level_2 AS version,
    country,
    IF(hit_number = first_interaction AND bounces = 1, TRUE, FALSE) AS is_bounce
  FROM
    `moz-fx-data-marketing-prod.ga_derived.www_site_hits_v1`
  WHERE
    date = @submission_date
    AND page_name LIKE '/firefox/%/whatsnew%'
    AND hit_type = 'PAGE'
    AND page_level_1 = 'firefox'
    AND page_level_3 = 'whatsnew'
)
SELECT
  date,
  country,
  locale,
  version,
  COUNT(DISTINCT visit_identifier) AS visits,
  COUNT(DISTINCT CASE WHEN is_bounce = TRUE THEN visit_identifier END) AS bounces
FROM
  wnp_visits
GROUP BY
  1,
  2,
  3,
  4
