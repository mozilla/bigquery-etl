--Get all page views with the page location, and a flag for whether it was an entrance or not to the session
WITH all_page_views AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS `date`,
    event_timestamp,
    user_pseudo_id || '-' || CAST(
      (
        SELECT
          `value`
        FROM
          UNNEST(event_params)
        WHERE
          key = 'ga_session_id'
        LIMIT
          1
      ).int_value AS STRING
    ) AS visit_identifier,
    device.category AS device_category,
    device.operating_system AS operating_system,
    device.web_info.browser AS browser,
    device.language AS `language`,
    geo.country AS country,
    collected_traffic_source.manual_source AS source,
    collected_traffic_source.manual_medium AS medium,
    collected_traffic_source.manual_campaign_name AS campaign,
    collected_traffic_source.manual_content AS content,
    (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'page_location'
      LIMIT
        1
    ).string_value AS page_location,
    (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'entrances'
      LIMIT
        1
    ).int_value AS is_entrance
  FROM
    `moz-fx-data-marketing-prod.analytics_314399816.events_*`
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
    AND event_name = 'page_view'
),
--Filter to entrance pages only, and then filter to ensure only 1 entrance page per session
--Theoretically Google should always only send 1 per session, but in case there is ever more than 1, which happens occasionally
entrance_page_views_only AS (
  SELECT
    `date`,
    visit_identifier,
    device_category,
    operating_system,
    browser,
    `language`,
    country,
    source,
    medium,
    campaign,
    content,
    SPLIT(REGEXP_REPLACE(SPLIT(page_location, '?')[SAFE_OFFSET(0)], '^https://', ''), '/')[
      SAFE_OFFSET(1)
    ] AS level_1,
    SPLIT(REGEXP_REPLACE(SPLIT(page_location, '?')[SAFE_OFFSET(0)], '^https://', ''), '/')[
      SAFE_OFFSET(2)
    ] AS level_2
  FROM
    all_page_views
  WHERE
    is_entrance = 1
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY visit_identifier ORDER BY event_timestamp ASC) = 1
),
staging AS (
  SELECT
    epvo.date,
    epvo.visit_identifier,
    epvo.device_category,
    epvo.operating_system,
    epvo.browser,
    epvo.language,
    epvo.country,
    epvo.source,
    epvo.medium,
    epvo.campaign,
    epvo.content,
    epvo.level_1,
    epvo.level_2,
    COUNT(DISTINCT(visit_identifier)) AS sessions
  FROM
    entrance_page_views_only epvo
  GROUP BY
    epvo.date,
    epvo.visit_identifier,
    epvo.device_category,
    epvo.operating_system,
    epvo.browser,
    epvo.language,
    epvo.country,
    epvo.source,
    epvo.medium,
    epvo.campaign,
    epvo.content,
    epvo.level_1,
    epvo.level_2
)
SELECT
  `date`,
  visit_identifier,
  device_category,
  operating_system,
  browser,
  `language`,
  country,
  source,
  medium,
  campaign,
  content,
  CASE
    WHEN LOWER(level_1) LIKE "press%"
      THEN "press"
    WHEN LOWER(level_1) = 'firefox'
      THEN 'The Firefox Frontier'
    WHEN level_1 = 'netPolicy'
      THEN 'Open Policy & Advocacy'
    WHEN LOWER(level_1) = 'internetcitizen'
      THEN 'Internet Citizen'
    WHEN LOWER(level_1) = 'futurereleases'
      THEN 'Future Releases'
    WHEN LOWER(level_1) = 'careers'
      THEN 'Careers'
    WHEN LOWER(level_1) = 'opendesign'
      THEN 'Open Design'
    WHEN level_1 = ""
      THEN "Blog Home Page"
    WHEN LOWER(level_1) IN (
        'blog',
        'addons',
        'security',
        'opendesign',
        'nnethercote',
        'thunderbird',
        'community',
        'l10n',
        'theden',
        'webrtc',
        'berlin',
        'webdev',
        'services',
        'tanvi',
        'laguaridadefirefox',
        'ux',
        'fxtesteng',
        'foundation-archive',
        'nfroyd',
        'sumo',
        'javascript',
        'page',
        'data'
      )
      THEN LOWER(level_1)
    ELSE 'other'
  END AS blog,
  CASE
    WHEN LOWER(level_1) = "firefox"
      AND LOWER(level_2) IN ('ru', 'pt-br', 'pl', 'it', 'id', 'fr', 'es', 'de')
      THEN LOWER(level_2)
    WHEN LOWER(level_1) = "firefox"
      THEN "Main"
    WHEN LOWER(level_1) LIKE "press-%"
      AND LOWER(level_1) IN (
        'press-de',
        'press-fr',
        'press-es',
        'press-uk',
        'press-pl',
        'press-it',
        'press-br',
        'press-nl'
      )
      THEN LOWER(level_1)
    WHEN LOWER(level_1) LIKE "press%"
      THEN "Main"
    WHEN LOWER(level_1) = 'internetcitizen'
      AND LOWER(level_2) IN ('de', 'fr')
      THEN LOWER(level_2)
    ELSE "Main"
  END AS subblog,
  `sessions`
FROM
  staging
