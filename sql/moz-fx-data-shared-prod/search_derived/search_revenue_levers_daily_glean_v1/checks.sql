#warn
-- monetizable_sap is rewritten on partner_code, so a Google code the rule does not list counts as not
-- monetized. Today about 96% of Google desktop SAP is monetizable; a drop below 90% means a new code,
-- such as a US variant of the new Google codes, needs adding to google_code_class in query.sql.
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
-- The new Google codes count as not monetized until their rule is settled (the NEW class in
-- google_code_class in query.sql). Warn once they carry more than 1% of Google desktop SAP, so the
-- rule is settled before it affects monetizable_sap. This reads the source aggregate, as the
-- output table has no partner_code.
WITH google_desktop AS (
  SELECT
    SUM(sap_kpi) AS sap,
    SUM(
      IF(
        REGEXP_CONTAINS(
          partner_code,
          r'^(android|ios|linux|macos|windows)-firefox(mnt)?-[a-z]+-re$'
        ),
        sap_kpi,
        0
      )
    ) AS new_code_sap
  FROM
    `moz-fx-data-shared-prod.search.search_aggregates_glean`
  WHERE
    submission_date = @submission_date
    AND normalized_engine = 'Google'
)
SELECT
  IF(
    SAFE_DIVIDE(new_code_sap, sap) > 0.01,
    ERROR(
      FORMAT(
        "New Google codes carry %.1f%% of Google desktop SAP (%d of %d). Settle their monetization rule in google_code_class.",
        100 * SAFE_DIVIDE(new_code_sap, sap),
        new_code_sap,
        sap
      )
    ),
    NULL
  )
FROM
  google_desktop;
