SELECT
  submission_date,
  EXTRACT(YEAR FROM submission_date) AS calendar_year,
  EXTRACT(MONTH FROM submission_date) AS calendar_month,
  CASE
    WHEN EXTRACT(MONTH FROM submission_date)
      BETWEEN 1
      AND 3
      THEN 1
    WHEN EXTRACT(MONTH FROM submission_date)
      BETWEEN 4
      AND 6
      THEN 2
    WHEN EXTRACT(MONTH FROM submission_date)
      BETWEEN 7
      AND 9
      THEN 3
    WHEN EXTRACT(MONTH FROM submission_date)
      BETWEEN 10
      AND 12
      THEN 4
    ELSE NULL
  END AS calendar_quarter,
  DATE(
    EXTRACT(YEAR FROM submission_date),
    EXTRACT(MONTH FROM submission_date),
    1
  ) AS first_date_of_month,
  CASE
    WHEN EXTRACT(MONTH FROM submission_date)
      BETWEEN 1
      AND 3
      THEN DATE(EXTRACT(YEAR FROM submission_date), 1, 1)
    WHEN EXTRACT(MONTH FROM submission_date)
      BETWEEN 4
      AND 6
      THEN DATE(EXTRACT(YEAR FROM submission_date), 4, 1)
    WHEN EXTRACT(MONTH FROM submission_date)
      BETWEEN 7
      AND 9
      THEN DATE(EXTRACT(YEAR FROM submission_date), 7, 1)
    WHEN EXTRACT(MONTH FROM submission_date)
      BETWEEN 10
      AND 12
      THEN DATE(EXTRACT(YEAR FROM submission_date), 10, 1)
    ELSE NULL
  END AS first_date_of_quarter
FROM
  UNNEST(GENERATE_DATE_ARRAY('1998-03-31', '2100-12-31')) AS submission_date
