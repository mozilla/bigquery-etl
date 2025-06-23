CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.external.holidays`
AS
WITH staging AS (
  SELECT
    submission_date,
    IF(calendar_month = 1 AND EXTRACT(day FROM submission_date) = 1, 1, 0) AS new_years_day,
    IF(calendar_month = 1 AND EXTRACT(day FROM submission_date) = 26, 1, 0) AS australia_day,
    IF(calendar_month = 1 AND EXTRACT(day FROM submission_date) = 1, 1, 0) AS orthodox_christmas,
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
      1,
      0
    ) AS lunar_new_year,
    IF(calendar_month = 1 AND EXTRACT(day FROM submission_date) = 26, 1, 0) AS in_republic_day,
    IF(
      submission_date IN (
        '2020-02-02',
        '2021-02-07',
        '2022-02-13',
        '2023-02-12',
        '2024-02-11',
        '2025-02-09',
        '2026-02-08',
        '2027-02-14',
        '2028-02-13',
        '2029-02-11'
      ),
      1,
      0
    ) AS us_superbowl,
    IF(calendar_month = 2 AND EXTRACT(day FROM submission_date) = 14, 1, 0) AS valentines_day,
    IF(
      submission_date IN (
        '2020-02-17',
        '2021-02-15',
        '2022-02-21',
        '2023-02-20',
        '2024-02-19',
        '2025-02-17',
        '2026-02-16',
        '2027-02-15',
        '2028-02-21',
        '2029-02-19'
      ),
      1,
      0
    ) AS us_presidents_day,
    IF(
      submission_date IN (
        '2020-03-19',
        '2021-03-20',
        '2022-03-20',
        '2023-03-20',
        '2024-03-19',
        '2025-03-20',
        '2026-03-20',
        '2027-03-20',
        '2028-03-19',
        '2029-03-20'
      ),
      1,
      0
    ) AS nowruz,
    IF(
      submission_date IN (
        '2020-03-10',
        '2021-03-29',
        '2022-03-18',
        '2023-03-08',
        '2024-03-25',
        '2025-03-14',
        '2026-03-04',
        '2027-03-22',
        '2028-03-11',
        '2029-03-01'
      ),
      1,
      0
    ) AS in_holi,
    IF(
      submission_date IN (
        '2020-05-23',
        '2020-05-24',
        '2021-05-12',
        '2021-05-13',
        '2022-05-01',
        '2022-05-02',
        '2023-04-20',
        '2023-04-21',
        '2024-04-09',
        '2024-04-10',
        '2025-03-29',
        '2025-03-30',
        '2026-05-19',
        '2026-05-20',
        '2027-03-09',
        '2027-03-10',
        '2028-02-26',
        '2028-02-27',
        '2029-02-14',
        '2029-02-15'
      ),
      1,
      0
    ) AS eid_al_fitr,
    IF(
      submission_date IN (
        '2020-04-17',
        '2021-04-30',
        '2022-04-22',
        '2023-04-14',
        '2024-05-03',
        '2025-04-18',
        '2026-04-10',
        '2027-04-30',
        '2028-04-14',
        '2029-04-06'
      ),
      1,
      0
    ) AS good_friday_eastern,
    IF(
      submission_date IN (
        '2020-04-19',
        '2021-05-02',
        '2022-04-24',
        '2023-04-16',
        '2024-05-05',
        '2025-04-20',
        '2026-04-12',
        '2027-05-02',
        '2028-04-16',
        '2029-04-08'
      ),
      1,
      0
    ) AS easter_day_eastern,
    IF(
      submission_date IN (
        '2020-04-20',
        '2021-05-03',
        '2022-04-25',
        '2023-04-17',
        '2024-05-06',
        '2025-04-21',
        '2026-04-13',
        '2027-05-03',
        '2028-04-17',
        '2029-04-09'
      ),
      1,
      0
    ) AS easter_monday_eastern,
    IF(
      submission_date IN (
        '2020-04-10',
        '2021-04-02',
        '2022-04-15',
        '2023-04-07',
        '2024-03-29',
        '2025-04-18',
        '2026-04-03',
        '2027-03-26',
        '2028-04-14',
        '2029-03-30'
      ),
      1,
      0
    ) AS good_friday_western,
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
      1,
      0
    ) AS easter_day_western,
    IF(
      submission_date IN (
        '2020-04-13',
        '2021-04-05',
        '2022-04-18',
        '2023-04-10',
        '2024-04-01',
        '2025-04-21',
        '2026-04-06',
        '2027-03-29',
        '2028-04-17',
        '2029-04-02'
      ),
      1,
      0
    ) AS easter_monday_western,
    IF(calendar_month = 4 AND EXTRACT(day FROM submission_date) = 21, 1, 0) AS br_tiradentes_day,
    IF(calendar_month = 4 AND EXTRACT(day FROM submission_date) = 25, 1, 0) AS au_anzac_day,
    IF(calendar_month = 5 AND EXTRACT(day FROM submission_date) = 1, 1, 0) AS fr_may_day,
    IF(calendar_month = 5 AND EXTRACT(day FROM submission_date) = 8, 1, 0) AS fr_victory_day,
    IF(calendar_month = 6 AND EXTRACT(day FROM submission_date) = 3, 1, 0) AS ir_death_of_khomeini,
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
      1,
      0
    ) AS us_memorial_day,
    IF(
      submission_date IN (
        '2020-05-21',
        '2021-05-13',
        '2022-05-26',
        '2023-05-18',
        '2024-05-09',
        '2025-05-29',
        '2026-05-14',
        '2027-05-06',
        '2028-05-25',
        '2029-05-10'
      ),
      1,
      0
    ) AS ascension_day,
    IF(
      submission_date IN (
        '2020-06-01',
        '2021-05-24',
        '2022-06-06',
        '2023-05-29',
        '2024-05-20',
        '2025-06-09',
        '2026-05-25',
        '2027-05-17',
        '2028-06-05',
        '2029-05-21'
      ),
      1,
      0
    ) AS whit_monday,
    IF(
      submission_date IN (
        '2020-07-30',
        '2020-07-31',
        '2021-07-19',
        '2021-07-20',
        '2022-07-08',
        '2022-07-09',
        '2023-06-27',
        '2023-06-28',
        '2024-07-16',
        '2024-07-17',
        '2025-06-06',
        '2025-06-07',
        '2026-05-26',
        '2026-05-27',
        '2027-05-16',
        '2027-05-17',
        '2028-05-04',
        '2028-05-05',
        '2029-04-23',
        '2029-04-24'
      ),
      1,
      0
    ) AS eid_al_adha,
    IF(
      submission_date IN (
        '2020-06-11',
        '2021-06-03',
        '2022-06-16',
        '2023-06-08',
        '2024-05-30',
        '2025-06-19',
        '2026-06-04',
        '2027-05-27',
        '2028-06-15',
        '2029-05-31'
      ),
      1,
      0
    ) AS corpus_christi,
    IF(calendar_month = 6 AND EXTRACT(day FROM submission_date) = 19, 1, 0) AS us_juneteenth,
    IF(calendar_month = 7 AND EXTRACT(day FROM submission_date) = 4, 1, 0) AS us_independence_day,
    IF(calendar_month = 7 AND EXTRACT(day FROM submission_date) = 14, 1, 0) AS fr_national_day,
    IF(
      submission_date IN (
        '2020-08-28',
        '2020-08-29',
        '2021-08-18',
        '2021-08-19',
        '2022-08-07',
        '2022-08-08',
        '2023-07-27',
        '2023-07-28',
        '2024-07-15',
        '2024-07-16',
        '2025-07-05',
        '2025-07-06',
        '2026-06-25',
        '2026-06-26',
        '2027-06-14',
        '2027-06-15',
        '2028-06-02',
        '2028-06-03',
        '2029-05-23',
        '2029-05-24'
      ),
      1,
      0
    ) AS ashura,
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
      1,
      0
    ) AS us_labor_day,
    IF(
      submission_date IN (
        '2020-10-30',
        '2021-10-19',
        '2022-10-08',
        '2023-09-28',
        '2024-09-16',
        '2025-09-05',
        '2026-08-26',
        '2027-08-14',
        '2028-08-02',
        '2029-07-23'
      ),
      1,
      0
    ) AS prophets_birthday,
    IF(calendar_month = 9 AND EXTRACT(day FROM submission_date) = 7, 1, 0) AS br_independence_day,
    IF(calendar_month = 9 AND EXTRACT(day FROM submission_date) = 16, 1, 0) AS mx_independence_day,
    IF(
      submission_date IN (
        '2020-11-26',
        '2021-11-25',
        '2022-11-24',
        '2023-11-23',
        '2024-11-28',
        '2025-11-27',
        '2026-11-26',
        '2027-11-25',
        '2028-11-23',
        '2029-11-22'
      ),
      1,
      0
    ) AS us_thanksgiving,
    IF(
      submission_date IN (
        '2020-11-27',
        '2021-11-26',
        '2022-11-25',
        '2023-11-24',
        '2024-11-29',
        '2025-11-28',
        '2026-11-27',
        '2027-11-26',
        '2028-11-24',
        '2029-11-23'
      ),
      1,
      0
    ) AS us_blackfriday,
    IF(
      submission_date IN (
        '2020-11-30',
        '2021-11-29',
        '2022-11-28',
        '2023-11-27',
        '2024-12-02',
        '2025-12-01',
        '2026-11-30',
        '2027-11-29',
        '2028-11-27',
        '2029-11-26'
      ),
      1,
      0
    ) AS us_cybermonday,
    IF(
      submission_date IN (
        '2020-10-12',
        '2021-10-11',
        '2022-10-10',
        '2023-10-09',
        '2024-10-14',
        '2025-10-13',
        '2026-10-12',
        '2027-10-11',
        '2028-10-09',
        '2029-10-08'
      ),
      1,
      0
    ) AS ca_thanksgiving,
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
        '2024-10-09',
        '2025-07-08',
        '2025-07-09',
        '2025-07-10',
        '2025-07-11'
      ),
      1,
      0
    ) AS amazon_prime_days_summer,
    IF(calendar_month = 10 AND EXTRACT(day FROM submission_date) = 3, 1, 0) AS de_unity_day,
    IF(
      submission_date IN (
        '2020-11-14',
        '2021-11-04',
        '2022-10-24',
        '2023-11-12',
        '2024-10-31',
        '2025-10-20',
        '2026-11-08',
        '2027-10-29',
        '2028-10-17',
        '2029-11-05'
      ),
      1,
      0
    ) AS in_diwali,
    IF(
      submission_date IN (
        '2020-10-25',
        '2021-10-15',
        '2022-10-05',
        '2023-10-24',
        '2024-10-12',
        '2025-10-02',
        '2026-10-20',
        '2027-10-09',
        '2028-09-27',
        '2029-10-16'
      ),
      1,
      0
    ) AS dussehra,
    IF(calendar_month = 11 AND EXTRACT(day FROM submission_date) = 11, 1, 0) AS us_veterans_day,
    IF(calendar_month = 11 AND EXTRACT(day FROM submission_date) = 20, 1, 0) AS mx_revolution_day,
    IF(calendar_month = 10 AND EXTRACT(day FROM submission_date) = 31, 1, 0) AS halloween,
    IF(calendar_month = 12 AND EXTRACT(day FROM submission_date) = 24, 1, 0) AS christmas_eve,
    IF(calendar_month = 12 AND EXTRACT(day FROM submission_date) = 25, 1, 0) AS christmas_day,
    IF(calendar_month = 12 AND EXTRACT(day FROM submission_date) = 26, 1, 0) AS boxing_day,
    CASE
      WHEN submission_date
        BETWEEN '2020-12-10'
        AND '2020-12-18'
        THEN 1
      WHEN submission_date
        BETWEEN '2021-11-28'
        AND '2021-12-06'
        THEN 1
      WHEN submission_date
        BETWEEN '2022-12-18'
        AND '2022-12-26'
        THEN 1
      WHEN submission_date
        BETWEEN '2023-12-07'
        AND '2023-12-15'
        THEN 1
      WHEN submission_date
        BETWEEN '2024-12-25'
        AND '2025-01-02'
        THEN 1
      WHEN submission_date
        BETWEEN '2025-12-14'
        AND '2025-12-22'
        THEN 1
      WHEN submission_date
        BETWEEN '2026-12-04'
        AND '2026-12-12'
        THEN 1
      WHEN submission_date
        BETWEEN '2027-12-24'
        AND '2028-01-01'
        THEN 1
      WHEN submission_date
        BETWEEN '2028-12-12'
        AND '2028-12-20'
        THEN 1
      WHEN submission_date
        BETWEEN '2029-12-01'
        AND '2029-12-09'
        THEN 1
      ELSE 0
    END AS hanukkah,
    CASE
      WHEN submission_date
        BETWEEN '2019-12-26'
        AND '2020-01-01'
        THEN 1
      WHEN submission_date
        BETWEEN '2020-12-26'
        AND '2021-01-01'
        THEN 1
      WHEN submission_date
        BETWEEN '2021-12-26'
        AND '2022-01-01'
        THEN 1
      WHEN submission_date
        BETWEEN '2022-12-26'
        AND '2023-01-01'
        THEN 1
      WHEN submission_date
        BETWEEN '2023-12-26'
        AND '2024-01-01'
        THEN 1
      WHEN submission_date
        BETWEEN '2024-12-26'
        AND '2025-01-01'
        THEN 1
      WHEN submission_date
        BETWEEN '2025-12-26'
        AND '2026-01-01'
        THEN 1
      WHEN submission_date
        BETWEEN '2026-12-26'
        AND '2027-01-01'
        THEN 1
      WHEN submission_date
        BETWEEN '2027-12-26'
        AND '2028-01-01'
        THEN 1
      WHEN submission_date
        BETWEEN '2028-12-26'
        AND '2029-01-01'
        THEN 1
      WHEN submission_date
        BETWEEN '2029-12-26'
        AND '2030-01-01'
        THEN 1
      ELSE 0
    END AS us_kwanzaa,
    IF(calendar_month = 12 AND EXTRACT(day FROM submission_date) = 31, 1, 0) AS new_years_eve,
  FROM
    `moz-fx-data-shared-prod.external_derived.calendar_v1`
  WHERE
    submission_date
    BETWEEN '2020-01-01'
    AND '2029-12-31' --only included holidays for this date range for now
)
SELECT
  stg.submission_date,
  stg.new_years_day,
  stg.australia_day,
  stg.orthodox_christmas,
  stg.lunar_new_year,
  stg.in_republic_day,
  stg.us_superbowl,
  stg.valentines_day,
  stg.us_presidents_day,
  stg.nowruz,
  stg.in_holi,
  stg.eid_al_fitr,
  stg.good_friday_eastern,
  stg.easter_day_eastern,
  stg.easter_monday_eastern,
  stg.good_friday_western,
  stg.easter_day_western,
  stg.easter_monday_western,
  stg.br_tiradentes_day,
  stg.au_anzac_day,
  stg.fr_may_day,
  stg.fr_victory_day,
  stg.ir_death_of_khomeini,
  stg.us_memorial_day,
  stg.ascension_day,
  stg.whit_monday,
  stg.eid_al_adha,
  stg.corpus_christi,
  stg.us_juneteenth,
  stg.us_independence_day,
  stg.fr_national_day,
  stg.ashura,
  stg.us_labor_day,
  stg.prophets_birthday,
  stg.br_independence_day,
  stg.mx_independence_day,
  stg.us_thanksgiving,
  stg.us_blackfriday,
  stg.us_cybermonday,
  stg.ca_thanksgiving,
  stg.amazon_prime_days_summer,
  stg.de_unity_day,
  stg.in_diwali,
  stg.dussehra,
  stg.us_veterans_day,
  stg.mx_revolution_day,
  stg.halloween,
  stg.christmas_eve,
  stg.christmas_day,
  stg.boxing_day,
  stg.hanukkah,
  stg.us_kwanzaa,
  stg.new_years_eve,
  ARRAY_CONCAT(
    IF(stg.new_years_day = 1, ['NewYearsDay'], []),
    IF(stg.australia_day = 1, ['AustraliaDay'], []),
    IF(stg.orthodox_christmas = 1, ['OrthodoxChristmas'], []),
    IF(stg.lunar_new_year = 1, ['LunarNewYear'], []),
    IF(stg.in_republic_day = 1, ['IN_RepublicDay'], []),
    IF(stg.us_superbowl = 1, ['US_SuperBowl'], []),
    IF(stg.valentines_day = 1, ['ValentinesDay'], []),
    IF(stg.us_presidents_day = 1, ['US_PresidentsDay'], []),
    IF(stg.nowruz = 1, ['Nowruz'], []),
    IF(stg.in_holi = 1, ['IN_Holi'], []),
    IF(stg.eid_al_fitr = 1, ['EidAlFitr'], []),
    IF(stg.good_friday_eastern = 1, ['GoodFriday_Eastern'], []),
    IF(stg.easter_day_eastern = 1, ['EasterDay_Eastern'], []),
    IF(stg.easter_monday_eastern = 1, ['EasterMonday_Eastern'], []),
    IF(stg.good_friday_western = 1, ['GoodFriday_Western'], []),
    IF(stg.easter_day_western = 1, ['EasterDay_Western'], []),
    IF(stg.easter_monday_western = 1, ['EasterMonday_Western'], []),
    IF(stg.br_tiradentes_day = 1, ['BR_TiradentesDay'], []),
    IF(stg.au_anzac_day = 1, ['AU_AnzacDay'], []),
    IF(stg.fr_may_day = 1, ['FR_MayDay'], []),
    IF(stg.fr_victory_day = 1, ['FR_VictoryDay'], []),
    IF(stg.ir_death_of_khomeini = 1, ['IR_DeathOfKhomeini'], []),
    IF(stg.us_memorial_day = 1, ['US_MemorialDay'], []),
    IF(stg.ascension_day = 1, ['AscensionDay'], []),
    IF(stg.whit_monday = 1, ['WhitMonday'], []),
    IF(stg.eid_al_adha = 1, ['EidAlAdha'], []),
    IF(stg.corpus_christi = 1, ['CorpusChristi'], []),
    IF(stg.us_juneteenth = 1, ['US_Juneteenth'], []),
    IF(stg.us_independence_day = 1, ['US_IndependenceDay'], []),
    IF(stg.fr_national_day = 1, ['FR_NationalDay'], []),
    IF(stg.ashura = 1, ['Ashura'], []),
    IF(stg.us_labor_day = 1, ['US_LaborDay'], []),
    IF(stg.prophets_birthday = 1, ['ProphetBirthday'], []),
    IF(stg.br_independence_day = 1, ['BR_IndependenceDay'], []),
    IF(stg.mx_independence_day = 1, ['MX_IndependenceDay'], []),
    IF(stg.us_thanksgiving = 1, ['US_Thanksgiving'], []),
    IF(stg.us_blackfriday = 1, ['US_BlackFriday'], []),
    IF(stg.us_cybermonday = 1, ['US_CyberMonday'], []),
    IF(stg.ca_thanksgiving = 1, ['CA_Thanksgiving'], []),
    IF(stg.amazon_prime_days_summer = 1, ['AmazonSummerPrimeDays'], []),
    IF(stg.de_unity_day = 1, ['DE_UnityDay'], []),
    IF(stg.in_diwali = 1, ['IN_Diwali'], []),
    IF(stg.dussehra = 1, ['Dussehra'], []),
    IF(stg.us_veterans_day = 1, ['US_VeteransDay'], []),
    IF(stg.mx_revolution_day = 1, ['MX_RevolutionDay'], []),
    IF(stg.halloween = 1, ['Halloween'], []),
    IF(stg.christmas_eve = 1, ['ChristmasEve'], []),
    IF(stg.christmas_day = 1, ['ChristmasDay'], []),
    IF(stg.boxing_day = 1, ['BoxingDay'], []),
    IF(stg.hanukkah = 1, ['Hanukkah'], []),
    IF(stg.us_kwanzaa = 1, ['US_Kwanzaa'], []),
    IF(stg.new_years_eve = 1, ['NewYearsEve'], [])
  ) AS holiday_array
FROM
  staging stg
