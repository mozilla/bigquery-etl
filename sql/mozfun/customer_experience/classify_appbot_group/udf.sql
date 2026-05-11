-- UDF to classify a Zendesk ticket by its aggregated tag set into an Appbot reporting group.
-- Note from the Customer Experience team:
-- This classification exists because we do not offer language support for email or app reviews.
-- All non-English tickets auto-close under a fake account named "SUMOJr"; agents never see them.
-- The 'Appbot - Non-English' bucket lets callers filter that out-of-scope work downstream.
CREATE OR REPLACE FUNCTION customer_experience.classify_appbot_group(tags ARRAY<STRING>)
RETURNS STRUCT<ticket_group STRING, is_bot BOOL, is_english BOOL> AS (
  IF(
    tags IS NULL,
    NULL,
    (
      SELECT
        STRUCT(
          CASE
            WHEN is_bot
              AND is_english
              THEN 'Appbot - English'
            WHEN is_bot
              AND NOT is_english
              THEN 'Appbot - Non-English'
            ELSE 'Other'
          END AS ticket_group,
          is_bot,
          is_english
        )
      FROM
        (
          SELECT
            IFNULL(LOGICAL_OR(tag = 'bot'), FALSE) AS is_bot,
            IFNULL(
              LOGICAL_OR(tag IN ('english', 'usa', 'unitedkingdom', 'canada', 'australia')),
              FALSE
            ) AS is_english
          FROM
            UNNEST(tags) AS tag
        )
    )
  )
);

-- Tests
SELECT
  -- ticket_group field
  assert.equals(
    'Appbot - English',
    customer_experience.classify_appbot_group(['bot', 'english']).ticket_group
  ),
  assert.equals(
    'Appbot - English',
    customer_experience.classify_appbot_group(['bot', 'usa']).ticket_group
  ),
  assert.equals(
    'Appbot - English',
    customer_experience.classify_appbot_group(['bot', 'unitedkingdom']).ticket_group
  ),
  assert.equals(
    'Appbot - English',
    customer_experience.classify_appbot_group(['bot', 'canada']).ticket_group
  ),
  assert.equals(
    'Appbot - English',
    customer_experience.classify_appbot_group(['bot', 'australia']).ticket_group
  ),
  assert.equals(
    'Appbot - English',
    customer_experience.classify_appbot_group(['bot', 'english', 'usa', 'canada']).ticket_group
  ),
  assert.equals(
    'Appbot - Non-English',
    customer_experience.classify_appbot_group(['bot', 'spanish']).ticket_group
  ),
  assert.equals(
    'Appbot - Non-English',
    customer_experience.classify_appbot_group(['bot']).ticket_group
  ),
  assert.equals('Other', customer_experience.classify_appbot_group(['english']).ticket_group),
  assert.equals(
    'Other',
    customer_experience.classify_appbot_group(['english', 'usa']).ticket_group
  ),
  assert.equals(
    'Other',
    customer_experience.classify_appbot_group(
      ['english', 'usa', 'unitedkingdom', 'canada', 'australia']
    ).ticket_group
  ),
  assert.equals('Other', customer_experience.classify_appbot_group(['spanish']).ticket_group),
  assert.equals(
    'Other',
    customer_experience.classify_appbot_group(['spanish', 'french']).ticket_group
  ),
  assert.equals('Other', customer_experience.classify_appbot_group([]).ticket_group),
  -- is_bot field
  assert.true(customer_experience.classify_appbot_group(['bot']).is_bot),
  assert.true(customer_experience.classify_appbot_group(['bot', 'english']).is_bot),
  assert.false(customer_experience.classify_appbot_group(['english']).is_bot),
  assert.false(customer_experience.classify_appbot_group([]).is_bot),
  -- is_english field
  assert.true(customer_experience.classify_appbot_group(['bot', 'english']).is_english),
  assert.true(customer_experience.classify_appbot_group(['unitedkingdom']).is_english),
  assert.false(customer_experience.classify_appbot_group(['bot']).is_english),
  assert.false(customer_experience.classify_appbot_group(['spanish']).is_english),
  assert.false(customer_experience.classify_appbot_group([]).is_english),
  -- NULL elements inside the array (LOGICAL_OR skips NULL comparisons; IFNULL pins flags to FALSE)
  assert.equals(
    'Other',
    customer_experience.classify_appbot_group([CAST(NULL AS STRING)]).ticket_group
  ),
  assert.false(customer_experience.classify_appbot_group([CAST(NULL AS STRING)]).is_bot),
  assert.false(customer_experience.classify_appbot_group([CAST(NULL AS STRING)]).is_english),
  assert.equals(
    'Appbot - Non-English',
    customer_experience.classify_appbot_group(['bot', CAST(NULL AS STRING)]).ticket_group
  ),
  assert.true(customer_experience.classify_appbot_group(['bot', CAST(NULL AS STRING)]).is_bot),
  assert.false(customer_experience.classify_appbot_group(['bot', CAST(NULL AS STRING)]).is_english),
  -- NULL input → NULL struct (and NULL fields)
  assert.null(customer_experience.classify_appbot_group(CAST(NULL AS ARRAY<STRING>))),
  assert.null(customer_experience.classify_appbot_group(CAST(NULL AS ARRAY<STRING>)).ticket_group),
  assert.null(customer_experience.classify_appbot_group(CAST(NULL AS ARRAY<STRING>)).is_bot),
  assert.null(customer_experience.classify_appbot_group(CAST(NULL AS ARRAY<STRING>)).is_english)
