WITH current_gclid_conversions AS (
  SELECT
    gclid,
    CASE
      WHEN conversion_event IN ('activation_1', 'activation_2')
        THEN first_week_end_date
      WHEN conversion_event IN ('first_day_DAU')
        THEN first_seen_date
      ELSE first_week_end_date
    END AS activity_date,
    conversion_event,
    has_conversion,
    marketing_card_optin_first_week,
  FROM
    `moz-fx-data-shared-prod.google_ads.fenix_conversion_event_categorization`
  CROSS JOIN
    UNNEST(
      [
        STRUCT('first_day_DAU' AS conversion_event, active_d0 AS has_conversion),
        STRUCT('activation_1' AS conversion_event, activation_1 AS has_conversion),
        STRUCT('activation_2' AS conversion_event, activation_2 AS has_conversion)
      ]
    )
  WHERE
    report_date = @submission_date
    AND gclid IS NOT NULL
  -- Right now we can have multiple clients associated with a gclid, order by client_id
  -- to make sure that every time we run the query we get the same result.
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY gclid, conversion_event ORDER BY client_id ASC) = 1
),
previous_gclid_conversions AS (
  SELECT
    gclid,
  FROM
    `moz-fx-data-shared-prod.google_ads_derived.fenix_gclid_conversions_v1`
  WHERE
    report_date < @submission_date
)
SELECT
  @submission_date AS report_date,
  current_gclid_conversions.gclid,
  current_gclid_conversions.conversion_event,
  current_gclid_conversions.activity_date,
FROM
  current_gclid_conversions
LEFT JOIN
  previous_gclid_conversions
  USING (gclid)
WHERE
  current_gclid_conversions.has_conversion = 1
  AND current_gclid_conversions.marketing_card_optin_first_week = 1
  AND previous_gclid_conversions.gclid IS NULL
