CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.org_mozilla_firefox.clients_last_seen`
AS
SELECT
  `moz-fx-data-shared-prod.udf.pos_of_trailing_set_bit`(baseline.days_seen_bits) AS days_since_seen,
  `moz-fx-data-shared-prod.udf.pos_of_trailing_set_bit`(
    baseline.days_seen_session_start_bits
  ) AS days_since_seen_session_start,
  `moz-fx-data-shared-prod.udf.pos_of_trailing_set_bit`(
    baseline.days_seen_session_end_bits
  ) AS days_since_seen_session_end,
  DATE_DIFF(submission_date, baseline.first_run_date, DAY) AS days_since_created_profile,
  * EXCEPT (baseline, metrics),
  baseline.*,
  metrics.*
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_derived.clients_last_seen_v1`
