CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozilla_org.ga_sessions`
AS
SELECT
  * REPLACE (
    mozfun.ga.nullify_string(campaign) AS campaign,
    mozfun.ga.nullify_string(source) AS source,
    mozfun.ga.nullify_string(medium) AS medium,
    mozfun.ga.nullify_string(term) AS term,
    mozfun.ga.nullify_string(content) AS content
  )
FROM
  `moz-fx-data-shared-prod.mozilla_org_derived.ga_sessions_v1`
