/*

Return the display name of the given os if name is recognized
Otherwise, return the given input

*/

CREATE TEMP FUNCTION udf_normalize_os_name(os_name STRING) AS ((
  SELECT
    IF(COUNT(*) = 0, os_name, udf_mode_last(ARRAY_AGG(display_family)))
  FROM
    UNNEST(udf_get_normalized_os_list())
  WHERE
    name = os_name
));

-- Test

SELECT
  assert_equals('Mac', udf_normalize_os_name('Darwin')),
  assert_equals('Windows', udf_normalize_os_name('Windows_NT')),
  assert_equals('Windows', udf_normalize_os_name('Windows_95')),
  assert_equals('Windows', udf_normalize_os_name('Windows_98')),
  assert_equals('Android', udf_normalize_os_name('Android')),
  assert_equals('Linux', udf_normalize_os_name('SunOS')),
  assert_equals('Linux', udf_normalize_os_name('OpenBSD')),
  assert_equals('Other', udf_normalize_os_name('Other'))
