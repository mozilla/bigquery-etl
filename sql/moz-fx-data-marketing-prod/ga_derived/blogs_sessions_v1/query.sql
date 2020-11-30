WITH with_hits AS (
  SELECT
    PARSE_DATE('%Y%m%d', date) AS date,
    CONCAT(CAST(fullVisitorId AS string), CAST(visitId AS string)) AS visit_identifier,
    device.deviceCategory AS device_category,
    device.operatingSystem AS operating_system,
    device.browser,
    device.language,
    geoNetwork.country AS country,
    trafficSource.source AS source,
    trafficSource.medium AS medium,
    trafficSource.campaign AS campaign,
    trafficSource.adcontent AS content,
    hits.page.pagePath AS landing_page,
    CASE
    WHEN
      hits.isEntrance IS NOT NULL
    THEN
      1
    ELSE
      0
    END
    AS entrance,
    SPLIT(hits.page.pagePathLevel1, '/')[SAFE_OFFSET(1)] AS blog,
    SPLIT(hits.page.pagePathLevel2, '/')[SAFE_OFFSET(1)] AS page_path_level2
  FROM
    `moz-fx-data-marketing-prod.66602784.ga_sessions_*`
  CROSS JOIN
    UNNEST(hits) AS hits
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
),
sessions_intermediate AS (
  SELECT
    * EXCEPT (blog, page_path_level2),
    CASE
    WHEN
      blog LIKE "press%"
    THEN
      "press"
    WHEN
      blog = 'firefox'
    THEN
      'The Firefox Frontier'
    WHEN
      blog = 'netPolicy'
    THEN
      'Open Policy & Advocacy'
    WHEN
      LOWER(blog) = 'internetcitizen'
    THEN
      'Internet Citizen'
    WHEN
      blog = 'futurereleases'
    THEN
      'Future Releases'
    WHEN
      blog = 'careers'
    THEN
      'Careers'
    WHEN
      blog = 'opendesign'
    THEN
      'Open Design'
    WHEN
      blog = ""
    THEN
      "Blog Home Page"
    WHEN
      LOWER(blog) IN (
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
    THEN
      LOWER(blog)
    ELSE
      'other'
    END
    AS blog,
    CASE
    WHEN
      blog = "firefox"
      AND page_path_level2 IN ('ru', 'pt-br', 'pl', 'it', 'id', 'fr', 'es', 'de')
    THEN
      page_path_level2
    WHEN
      blog = "firefox"
    THEN
      "Main"
    WHEN
      blog LIKE "press-%"
      AND blog IN (
        'press-de',
        'press-fr',
        'press-es',
        'press-uk',
        'press-pl',
        'press-it',
        'press-br',
        'press-nl'
      )
    THEN
      blog
    WHEN
      blog LIKE "press%"
    THEN
      "Main"
    WHEN
      blog = 'internetcitizen'
      AND page_path_level2 IN ('de', 'fr')
    THEN
      page_path_level2
    ELSE
      "Main"
    END
    AS subblog,
    ROW_NUMBER() OVER (
      PARTITION BY
        visit_identifier
      ORDER BY
        visit_identifier,
        entrance
    ) AS entry_page,
    COUNT(DISTINCT visit_identifier) AS sessions
  FROM
    with_hits
  GROUP BY
    date,
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
    landing_page,
    blog,
    subblog,
    entrance
)
SELECT
  date,
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
  blog,
  subblog,
  SUM(sessions) AS sessions,
FROM
  sessions_intermediate
WHERE
  entry_page = 1
GROUP BY
  date,
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
  blog,
  subblog
