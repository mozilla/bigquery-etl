--Unable to do normal data check since "adm_daily_aggregates_v1" passes in parameter ALLOW_FIELD_EDITION,
--so this is a separate task to check each day has at least 100 rows and to fail the Airflow task if not
SELECT
  submission_date,
  COUNT(1) AS num_rows
FROM
  `moz-fx-data-shared-prod.search_terms_derived.adm_daily_aggregates_v1`
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date
HAVING
  COUNT(1) >= 100
