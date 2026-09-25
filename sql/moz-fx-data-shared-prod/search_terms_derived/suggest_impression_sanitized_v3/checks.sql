#fail
SELECT
  IF(
    COUNT(*) = 0,
    ERROR(
      "Merino sanitization job has not completed successfully for today. Wait for sanitization job to complete and re-run parent task."
    ),
    NULL
  )
FROM
  `moz-fx-data-shared-prod.search_terms_derived.sanitization_job_metadata_v2`
WHERE
  DATE(started_at) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
  AND status = 'SUCCESS';

-- Mozilla-supplied suggestions are excluded from the adMarketplace exports
-- (adm_daily_aggregates_v1, adm_daily_dma_aggregates_v1) by
-- udf.is_moz_supplied_suggestion, which matches the /v1/st endpoint on
-- ads.mozilla.org and ads.allizom.org. That filter fails open, so if the
-- reporting_url format changes outside this repo, Mozilla-supplied rows would
-- silently start reaching the partner. Fail the DAG instead.

#fail
ASSERT (
  (
    SELECT
      COUNT(*)
    FROM
      `moz-fx-data-shared-prod.search_terms_derived.suggest_impression_sanitized_v3`
    WHERE
      DATE(submission_timestamp) = @submission_date
      AND LOWER(NET.REG_DOMAIN(reporting_url)) IN ('mozilla.org', 'allizom.org')
      AND NOT `moz-fx-data-shared-prod.udf.is_moz_supplied_suggestion`(reporting_url)
  ) = 0
)
AS
  "Found reporting_urls on a mozilla.org/allizom.org host that udf.is_moz_supplied_suggestion does not match. These Mozilla-supplied rows would be reported to adMarketplace; the reporting_url format has probably changed and both the UDF and this check need updating.";

-- Any other unrecognised host is only a warning: it is more likely to be a new
-- adMarketplace reporting host than a Mozilla one, and the row is reported to
-- the partner either way, as it is today.

#warn
ASSERT (
  (
    SELECT
      COUNT(*)
    FROM
      `moz-fx-data-shared-prod.search_terms_derived.suggest_impression_sanitized_v3`
    WHERE
      DATE(submission_timestamp) = @submission_date
      AND reporting_url IS NOT NULL
      AND COALESCE(LOWER(NET.REG_DOMAIN(reporting_url)), '<unparseable>') NOT IN (
        'mt48.net',
        'admarketplace.net',
        'mozilla.org',
        'allizom.org'
      )
  ) = 0
)
AS
  "Found reporting_urls outside the known adMarketplace (mt48.net, admarketplace.net) and Mozilla (mozilla.org, allizom.org) domains. Confirm whether these should be reported to adMarketplace and extend this check.";
