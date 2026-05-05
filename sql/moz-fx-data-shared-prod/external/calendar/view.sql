CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.external.calendar`
AS
SELECT
  submission_date,
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM submission_date) = 1
      THEN 'Sunday'
    WHEN EXTRACT(DAYOFWEEK FROM submission_date) = 2
      THEN 'Monday'
    WHEN EXTRACT(DAYOFWEEK FROM submission_date) = 3
      THEN 'Tuesday'
    WHEN EXTRACT(DAYOFWEEK FROM submission_date) = 4
      THEN 'Wednesday'
    WHEN EXTRACT(DAYOFWEEK FROM submission_date) = 5
      THEN 'Thursday'
    WHEN EXTRACT(DAYOFWEEK FROM submission_date) = 6
      THEN 'Friday'
    WHEN EXTRACT(DAYOFWEEK FROM submission_date) = 7
      THEN 'Saturday'
    ELSE NULL
  END AS day_of_week,
  calendar_year AS `year`,
  calendar_month AS `month`,
  calendar_quarter AS `quarter`,
  EXTRACT(WEEK FROM submission_date) AS week_number,
  DATE_TRUNC(submission_date, WEEK) AS first_date_of_week,
  first_date_of_month,
  first_date_of_quarter,
  DATE_TRUNC(submission_date, YEAR) AS first_date_of_year,
  EXTRACT(DAYOFYEAR FROM submission_date) AS day_of_year
FROM
  `moz-fx-data-shared-prod.external_derived.calendar_v1`
