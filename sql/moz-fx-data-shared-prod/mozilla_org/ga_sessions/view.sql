CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_org.ga_sessions`
AS
SELECT
  * REPLACE (
    mozdata.analysis.ga_nullify_string(campaign) AS campaign,
    mozdata.analysis.ga_nullify_string(source) AS source,
    mozdata.analysis.ga_nullify_string(medium) AS medium,
    mozdata.analysis.ga_nullify_string(term) AS term,
    mozdata.analysis.ga_nullify_string(content) AS content
  )
FROM
  `moz-fx-data-shared-prod.mozilla_org_derived.ga_sessions_v1`
