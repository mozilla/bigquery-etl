
#warn
SELECT
  IF(
    ROUND(
      (COUNTIF(NOT REGEXP_CONTAINS(visit_identifier, r"^[0-9]+\.{1}[0-9]+\-{1}[0-9]+$"))) / COUNT(
        *
      ) * 100,
      2
    ) > 0,
    ERROR("Warn - some visit_identifier not matching expected pattern"),
    NULL
  )
FROM
  `moz-fx-data-marketing-prod.ga_derived.www_site_hits_v2`
WHERE
  date = @submission_date;
