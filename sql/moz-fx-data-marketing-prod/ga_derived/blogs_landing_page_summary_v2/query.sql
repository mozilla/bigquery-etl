WITH landing_page_staging AS (
  --note to self - should check it's always only 1 row per date/visit identifier, so that the other things don't get double counted once joined to them
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
    ? AS landing_page,
    ? AS cleaned_landing_page,
    ? AS page_sessions,
  FROM
    `moz-fx-data-marketing-prod.analytics_314399816.events_*`
  WHERE
    _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d', @submission_date)
  GROUP BY
    `date`,
    visit_identifier,
    landing_page,
    cleaned_landing_page
  HAVING
    page_sessions > 0
)
SELECT
  `date`,
  device_category,
  operating_system,
  browser,
  `language`,
  country,
  standardized_country_list.standardized_country AS standardized_country_name,
  source,
  medium,
  campaign,
  content,
  blog,
  subblog,
  landing_page,
  cleaned_landing_page,
  SUM(sessions) AS sessions,
  SUM(downloads) AS downloads,
  SUM(social_share) AS social_share,
  SUM(newsletter_subscription) AS newsletter_subscription,
FROM
  `moz-fx-data-marketing-prod.ga_derived.blogs_sessions_v2` AS sessions_table
LEFT JOIN
  `moz-fx-data-marketing-prod.ga_derived.blogs_goals_v2` AS goals_table
  USING (date, visit_identifier)
LEFT JOIN
  landing_page_table
  USING (date, visit_identifier)
LEFT JOIN
  `moz-fx-data-shared-prod.static.third_party_standardized_country_names` AS standardized_country_list
  ON sessions_table.country = standardized_country_list.raw_country
WHERE
  sessions_table.date = @submission_date
GROUP BY
  `date`,
  device_category,
  operating_system,
  browser,
  `language`,
  country,
  standardized_country_name,
  source,
  medium,
  campaign,
  content,
  blog,
  subblog,
  landing_page,
  cleaned_landing_page
