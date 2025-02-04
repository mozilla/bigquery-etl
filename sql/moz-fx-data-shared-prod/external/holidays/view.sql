CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.external.holidays`
AS
SELECT
  submission_date,
  IF(calendar_month = 1 AND EXTRACT(day FROM submission_date) = 1, TRUE, FALSE) AS new_years_day,
  IF(
    submission_date IN (
      '2020-01-25',
      '2021-02-12',
      '2022-02-01',
      '2023-01-22',
      '2024-02-10',
      '2025-01-29',
      '2026-02-17',
      '2027-02-06',
      '2028-01-26',
      '2029-02-13'
    ),
    TRUE,
    FALSE
  ) AS lunar_new_year,
  IF(calendar_month = 2 AND EXTRACT(day FROM submission_date) = 14, TRUE, FALSE) AS valentines_day,
  IF(
    submission_date IN (
      '2020-04-12',
      '2021-04-04',
      '2022-04-17',
      '2023-04-09',
      '2024-03-31',
      '2025-04-20',
      '2026-04-05',
      '2027-03-28',
      '2028-04-16',
      '2029-04-01'
    ),
    TRUE,
    FALSE
  ) AS easter_day,
  IF(
    submission_date IN (
      '2020-05-25',
      '2021-05-31',
      '2022-05-30',
      '2023-05-29',
      '2024-05-27',
      '2025-05-26',
      '2026-05-25',
      '2027-05-31',
      '2028-05-29',
      '2029-05-28'
    ),
    TRUE,
    FALSE
  ) AS us_memorial_day,
  IF(calendar_month = 6 AND EXTRACT(day FROM submission_date) = 19, TRUE, FALSE) AS us_juneteenth,
  IF(
    calendar_month = 7
    AND EXTRACT(day FROM submission_date) = 4,
    TRUE,
    FALSE
  ) AS us_independence_day,
  IF(
    submission_date IN (
      '2020-09-07',
      '2021-09-06',
      '2022-09-05',
      '2023-09-04',
      '2024-09-02',
      '2025-09-01',
      '2026-09-07',
      '2027-09-06',
      '2028-09-04',
      '2029-09-03'
    ),
    TRUE,
    FALSE
  ) AS us_labor_day,
    /*
  CASE
  WHEN submission_date IN () THEN TRUE 
  ELSE FALSE 
  END AS us_thanksgiving,
  CASE
  WHEN submission_date IN () THEN TRUE 
  ELSE FALSE 
  END AS us_blackfriday,
  CASE
  WHEN submission_date IN () THEN TRUE 
  ELSE FALSE 
  END AS us_cybermonday,
  CASE 
  WHEN submission_date IN () THEN TRUE 
  ELSE FALSE 
  END AS ca_thanksgiving,
  */
--future dates unknown currently, will add as they are announced
  IF(
    submission_date IN (
      '2020-10-13',
      '2020-10-14',
      '2021-06-21',
      '2021-06-22',
      '2021-10-10',
      '2021-10-11',
      '2022-07-12',
      '2022-07-13',
      '2022-10-11',
      '2022-10-12',
      '2023-07-11',
      '2023-07-12',
      '2023-10-10',
      '2023-10-11',
      '2024-07-16',
      '2024-07-17',
      '2024-10-08',
      '2024-10-09'
    ),
    TRUE,
    FALSE
  ) AS amazon_prime_days_summer,
  IF(calendar_month = 10 AND EXTRACT(day FROM submission_date) = 3, TRUE, FALSE) AS de_unity_day,
  IF(calendar_month = 10 AND EXTRACT(day FROM submission_date) = 31, TRUE, FALSE) AS halloween,
  IF(calendar_month = 12 AND EXTRACT(day FROM submission_date) = 24, TRUE, FALSE) AS christmas_eve,
  IF(calendar_month = 12 AND EXTRACT(day FROM submission_date) = 25, TRUE, FALSE) AS christmas_day,
  IF(calendar_month = 12 AND EXTRACT(day FROM submission_date) = 31, TRUE, FALSE) AS new_years_eve,
FROM
  `moz-fx-data-shared-prod.external_derived.calendar_v1`
WHERE
  submission_date
  BETWEEN '2020-01-01'
  AND '2029-12-31' --only included holidays for this date range for now
