CREATE TABLE IF NOT EXISTS
  `moz-fx-data-marketing-prod.ga_derived.blogs_goals_v1`(
    date DATE,
    visitIdentifier STRING,
    downloads INT64,
    socialShare INT64,
    newsletterSubscription INT64,
  )
PARTITION BY
  date
