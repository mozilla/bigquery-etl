#fail
-- The ratio checks below pass when a partition has no desktop rows, because SAFE_DIVIDE returns NULL.
-- A run before search_aggregates_glean is written for the day would publish mobile-only rows without
-- any error, so require desktop SAP for every partner.
SELECT
  IF(
    COUNT(DISTINCT IF(device = 'desktop' AND sap > 0, partner, NULL)) < 3,
    ERROR(
      FORMAT(
        "Only %d of 3 partners have desktop SAP for %t. Check that search_aggregates_glean is written for the day.",
        COUNT(DISTINCT IF(device = 'desktop' AND sap > 0, partner, NULL)),
        @submission_date
      )
    ),
    NULL
  )
FROM
  `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
WHERE
  submission_date = @submission_date;

#warn
-- monetizable_sap is rewritten on partner_code, so a Google code the rule does not list counts as not
-- monetized. Today about 96% of Google desktop SAP is monetizable; a drop below 90% means a new code
-- needs adding to google_code_class in query.sql.
WITH google_desktop AS (
  SELECT
    SUM(sap) AS sap,
    SUM(monetizable_sap) AS monetizable_sap
  FROM
    `{{ project_id }}.{{ dataset_id }}.{{ table_name }}`
  WHERE
    submission_date = @submission_date
    AND partner = 'Google'
    AND device = 'desktop'
)
SELECT
  IF(
    SAFE_DIVIDE(monetizable_sap, sap) < 0.9,
    ERROR(
      FORMAT(
        "Only %.1f%% of Google desktop SAP is monetizable (%d of %d). Check for a partner_code that google_code_class does not list.",
        100 * SAFE_DIVIDE(monetizable_sap, sap),
        monetizable_sap,
        sap
      )
    ),
    NULL
  )
FROM
  google_desktop;

#warn
-- A Google partner_code that google_code_class in query.sql does not list counts as not monetized.
-- Warn when a code outside the known set (the listed RoW, US and Mint codes plus no_code, ubuntu and
-- ubuntu-sn, which are known and not monetized) carries more than 1% of Google desktop SAP, such as
-- a new Google code, so its rule is settled before it is missed in monetizable_sap. This reads the
-- source aggregate, as the output table has no partner_code, and only the countries Google is eligible
-- for, the same population as the table (the list is the one in google_eligible_country in query.sql).
WITH google_desktop AS (
  SELECT
    SUM(sap_kpi) AS sap,
    SUM(
      IF(
        partner_code NOT IN (
          'firefox-b-d',
          'firefox-b-e',
          'firefox-b-1-d',
          'firefox-b-1-e',
          'firefox-b-lm',
          'firefox-b-1-lm',
          'no_code',
          'ubuntu',
          'ubuntu-sn'
        ),
        sap_kpi,
        0
      )
    ) AS unlisted_sap
  FROM
    `moz-fx-data-shared-prod.search.search_aggregates_glean`
  WHERE
    submission_date = @submission_date
    AND normalized_engine = 'Google'
    AND (
      (submission_date < '2023-12-01' AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
      OR (submission_date >= '2023-12-01' AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
    )
)
SELECT
  IF(
    SAFE_DIVIDE(unlisted_sap, sap) > 0.01,
    ERROR(
      FORMAT(
        "Google partner codes that google_code_class does not list carry %.1f%% of Google desktop SAP (%d of %d). Settle their monetization rule in google_code_class.",
        100 * SAFE_DIVIDE(unlisted_sap, sap),
        unlisted_sap,
        sap
      )
    ),
    NULL
  )
FROM
  google_desktop;

#warn
-- Bing and DuckDuckGo are monetized on the partner_codes listed in other_partner_code_monetized in
-- query.sql, and a code that is not listed counts as not monetized. Warn when a code outside the
-- known set (the listed codes plus no_code and lm) carries more than 1% of either partner's desktop
-- SAP, so it gets a rule before it is missed in monetizable_sap. This reads the source aggregate,
-- as the output table has no partner_code.
WITH other_partners AS (
  SELECT
    normalized_engine,
    SUM(sap_kpi) AS sap,
    SUM(
      IF(partner_code NOT IN ('MOZI', 'MOZR', 'ffab', 'ftsa', 'no_code', 'lm'), sap_kpi, 0)
    ) AS unlisted_sap
  FROM
    `moz-fx-data-shared-prod.search.search_aggregates_glean`
  WHERE
    submission_date = @submission_date
    AND normalized_engine IN ('Bing', 'DuckDuckGo')
  GROUP BY
    normalized_engine
)
SELECT
  IF(
    COUNTIF(SAFE_DIVIDE(unlisted_sap, sap) > 0.01) > 0,
    ERROR(
      FORMAT(
        "Partner codes that other_partner_code_monetized does not list carry over 1%% of desktop SAP for: %s. Add a rule for them in query.sql.",
        STRING_AGG(IF(SAFE_DIVIDE(unlisted_sap, sap) > 0.01, normalized_engine, NULL), ', ')
      )
    ),
    NULL
  )
FROM
  other_partners;

#warn
-- no_code is in the known set of the unlisted-code checks, so a regression upstream that empties
-- partner_code would move volume into no_code, which is not monetized, without those checks noticing.
-- Warn when no_code carries more than 2% of Google desktop SAP in the eligible countries, where it is
-- about 0.6% to 0.8% a day. This reads the source aggregate, as the output table has no partner_code.
WITH google_desktop AS (
  SELECT
    SUM(sap_kpi) AS sap,
    SUM(IF(partner_code = 'no_code', sap_kpi, 0)) AS no_code_sap
  FROM
    `moz-fx-data-shared-prod.search.search_aggregates_glean`
  WHERE
    submission_date = @submission_date
    AND normalized_engine = 'Google'
    AND (
      (submission_date < '2023-12-01' AND country NOT IN ('RU', 'UA', 'TR', 'BY', 'KZ', 'CN'))
      OR (submission_date >= '2023-12-01' AND country NOT IN ('RU', 'UA', 'BY', 'CN'))
    )
)
SELECT
  IF(
    SAFE_DIVIDE(no_code_sap, sap) > 0.02,
    ERROR(
      FORMAT(
        "no_code carries %.1f%% of Google desktop SAP in the eligible countries (%d of %d). Check partner_code upstream.",
        100 * SAFE_DIVIDE(no_code_sap, sap),
        no_code_sap,
        sap
      )
    ),
    NULL
  )
FROM
  google_desktop;
