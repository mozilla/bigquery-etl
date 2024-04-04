WITH aggregated_search_terms AS (
  SELECT
    DATE(submission_timestamp) AS submission_date,
    sanitized_query AS search_terms,
    COUNT(*) AS impressions,
    COUNTIF(is_clicked) AS clicks,
    COUNT(DISTINCT context_id) AS client_days
  FROM
    search_terms_derived.suggest_impression_sanitized_v2
  WHERE
    DATE(submission_timestamp) = @submission_date
  GROUP BY
    submission_date,
    search_terms
),
suggest_attributes AS (
  SELECT
    search_terms,
    advertiser,
    iab_category,
    title,
  FROM
    search_terms_derived.remotesettings_suggestions_v1 AS t
  CROSS JOIN
    t.keywords AS search_terms
)
SELECT
  *
FROM
  aggregated_search_terms
LEFT JOIN
  suggest_attributes
  USING (search_terms)
