SELECT
  date,
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
  SUM(sessions) AS sessions,
  SUM(downloads) AS downloads,
  SUM(social_share) AS social_share,
  SUM(newsletter_subscription) AS newsletter_subscription,
FROM
  `moz-fx-data-marketing-prod.ga_derived.blogs_sessions_v1` AS sessions_table
LEFT JOIN
  `moz-fx-data-marketing-prod.ga_derived.blogs_goals_v1`
  USING (date, visit_identifier)
LEFT JOIN
  `moz-fx-data-shared-prod.static.third_party_standardized_country_names` AS standardized_country_list
  ON sessions_table.country = standardized_country_list.raw_country
WHERE
  date = @submission_date
GROUP BY
  date,
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
  subblog
