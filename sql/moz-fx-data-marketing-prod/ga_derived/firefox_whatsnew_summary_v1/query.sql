WITH wnp_visits AS (
  SELECT
    date,
    visit_identifier,
    TRIM(page_path_level1, '/') AS locale,
    page_level_2 AS version,
    mozfun.norm.browser_version_info(page_level_2) AS version_info,
    country,
    IF(hit_number = first_interaction AND bounces = 1, TRUE, FALSE) AS is_bounce
  FROM
    `moz-fx-data-marketing-prod.ga_derived.www_site_hits_v1`
  WHERE
    date = @submission_date
    AND hit_type = 'PAGE'
    -- Match page paths like "/{locale}/firefox/{version}/whatsnew/..."
    -- Version regular expression is adapted from https://github.com/mozilla/bedrock/blob/main/bedrock/releasenotes/__init__.py
    AND page_level_1 = 'firefox'
    AND REGEXP_CONTAINS(page_level_2, r'^\d{1,3}(\.\d{1,3}){1,3}((a|b(eta)?)\d*)?(pre\d*)?(esr)?$')
    AND page_level_3 = 'whatsnew'
)
SELECT
  date,
  country,
  locale,
  version,
  version_info.major_version,
  version_info.minor_version,
  version_info.patch_revision,
  version_info.is_major_release,
  COUNT(DISTINCT visit_identifier) AS visits,
  COUNT(DISTINCT CASE WHEN is_bounce = TRUE THEN visit_identifier END) AS bounces
FROM
  wnp_visits
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8
