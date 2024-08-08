WITH landing_page_staging AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS `date`,
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
    (
      SELECT
        `value`
      FROM
        UNNEST(event_params)
      WHERE
        key = 'page_location'
      LIMIT
        1
    ).string_value AS landing_page,
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
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY visit_identifier ORDER BY event_timestamp ASC) = 1
),
landing_page_stg AS (
  SELECT
    `date`,
    visit_identifier,
    landing_page,
    SPLIT(landing_page, '?')[OFFSET(0)] AS cleaned_landing_page,
    SUM(is_entrance) AS page_sessions
  FROM
    landing_page_staging
  GROUP BY
    `date`,
    visit_identifier,
    landing_page,
    cleaned_landing_page
  HAVING
    page_sessions > 0
)
SELECT
  sessions_table.`date`,
  sessions_table.device_category,
  sessions_table.operating_system,
  sessions_table.browser,
  sessions_table.`language`,
  sessions_table.country,
  standardized_country_list.standardized_country AS standardized_country_name,
  sessions_table.source,
  sessions_table.medium,
  sessions_table.campaign,
  sessions_table.content,
  sessions_table.blog,
  sessions_table.subblog,
  lps.landing_page,
  lps.cleaned_landing_page,
  SUM(sessions_table.sessions) AS sessions,
  SUM(goals_table.downloads) AS downloads,
  SUM(goals_table.social_share) AS social_share,
  SUM(goals_table.newsletter_subscription) AS newsletter_subscription,
FROM
  `moz-fx-data-shared-prod.mozilla_org_derived.blogs_sessions_v2` AS sessions_table
LEFT JOIN
  `moz-fx-data-shared-prod.mozilla_org_derived.blogs_goals_v2` AS goals_table
  USING (`date`, visit_identifier)
LEFT JOIN
  landing_page_stg AS lps
  USING (`date`, visit_identifier)
LEFT JOIN
  `moz-fx-data-shared-prod.static.third_party_standardized_country_names` AS standardized_country_list
  ON sessions_table.country = standardized_country_list.raw_country
WHERE
  sessions_table.date = @submission_date
GROUP BY
  sessions_table.`date`,
  sessions_table.device_category,
  sessions_table.operating_system,
  sessions_table.browser,
  sessions_table.`language`,
  sessions_table.country,
  standardized_country_list.standardized_country,
  sessions_table.source,
  sessions_table.medium,
  sessions_table.campaign,
  sessions_table.content,
  sessions_table.blog,
  sessions_table.subblog,
  lps.landing_page,
  lps.cleaned_landing_page
