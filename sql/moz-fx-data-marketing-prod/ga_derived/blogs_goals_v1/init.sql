CREATE TABLE IF NOT EXISTS
  `moz-fx-data-marketing-prod.ga_derived.blogs_goals_v1`(
    date DATE,
    visit_identifier STRING,
    downloads INT64,
    social_share INT64,
    newsletter_subscription INT64,
  )
PARTITION BY
  date
