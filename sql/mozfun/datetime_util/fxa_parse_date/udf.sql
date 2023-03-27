CREATE OR REPLACE FUNCTION datetime_util.fxa_parse_date(date_string STRING)
RETURNS DATE AS (
  CASE
    WHEN REGEXP_CONTAINS(date_string, r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{1,3}Z)$")
      THEN DATE(DATETIME(SPLIT(date_string, ".")[OFFSET(0)]))
    WHEN REGEXP_CONTAINS(date_string, r"^(\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}\s(AM|PM))$")
      THEN DATE(PARSE_DATETIME("%m/%d/%Y %I:%M %p", date_string))
    WHEN REGEXP_CONTAINS(date_string, r"^(\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}:\d{3})$")
      THEN DATE(PARSE_DATETIME("%m/%d/%Y", SPLIT(date_string, " ")[OFFSET(0)]))
    WHEN REGEXP_CONTAINS(date_string, r"^(\d{2}/\d{2}/\d{4}\s\d{2}:\d{2}:\d{2})$")
      THEN DATE(PARSE_DATETIME("%m/%d/%Y", SPLIT(date_string, " ")[OFFSET(0)]))
    WHEN REGEXP_CONTAINS(date_string, r"^(\d{2}/\d{2}/\d{4})$")
      THEN PARSE_DATE("%m/%d/%Y", date_string)
    WHEN REGEXP_CONTAINS(date_string, r"^(\d{4}/\d{2}/\d{2})$")
      THEN PARSE_DATE("%Y/%m/%d", date_string)
    ELSE DATE(NULLIF(date_string, ''))
  END
);

-- Tests
SELECT
  assert.equals(DATE("2021-01-01"), mozfun.datetime_util.fxa_parse_date("2021-01-01")),
  assert.equals(DATE("2021-01-01"), mozfun.datetime_util.fxa_parse_date("2021/01/01")),
  assert.equals(DATE("2020-09-23"), mozfun.datetime_util.fxa_parse_date("09/23/2020")),
  assert.equals(DATE("2017-04-23"), mozfun.datetime_util.fxa_parse_date("04/23/2017")),
  assert.equals(DATE("2022-04-21"), mozfun.datetime_util.fxa_parse_date("2022-04-21 12:43:34 UTC")),
  assert.equals(
    DATE("2022-04-21"),
    mozfun.datetime_util.fxa_parse_date("2022-04-21 12:44:36.707000 UTC")
  ),
  assert.equals(DATE("2022-04-21"), mozfun.datetime_util.fxa_parse_date("04/21/2022 09:36 PM")),
  assert.equals(DATE("2022-04-21"), mozfun.datetime_util.fxa_parse_date("04/21/2022 09:36 AM")),
  assert.equals(DATE("2022-02-28"), mozfun.datetime_util.fxa_parse_date("02/28/2022 03:33:172")),
  assert.equals(DATE("2022-04-13"), mozfun.datetime_util.fxa_parse_date("04/13/2022 21:13:54")),
