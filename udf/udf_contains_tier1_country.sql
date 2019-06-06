CREATE TEMP FUNCTION
  udf_contains_tier1_country(x ANY TYPE) AS ( --
    EXISTS(
    SELECT
      country
    FROM
      UNNEST(x) AS country
    WHERE
      country IN ( --
        'United States',
        'France',
        'Germany',
        'United Kingdom',
        'Canada')) );