/*

Return the display version of the given os and version combination if
the combination is recognized
If the os name is not in the list or the os name is associated with a null version,
return the given version
If the os name is in the list but the version is not, return null

*/

CREATE TEMP FUNCTION udf_normalize_os_version(os_name STRING, os_version STRING) AS ((
  WITH version_list AS (
    SELECT
      *
      REPLACE (
        CASE
          WHEN version IS NULL THEN os_version
          WHEN os_name = 'Darwin'
            AND REGEXP_CONTAINS(os_version, CONCAT('^', version))
            THEN display_version
          WHEN os_name != 'Darwin'
            AND os_version = version
            THEN display_version
        END AS display_version
      )
    FROM
      UNNEST(udf_get_normalized_os_list())
    WHERE
      os_name = name
  )
  SELECT
    IF(COUNT(*) = 0, os_version, udf_mode_last(ARRAY_AGG(display_version)))
  FROM
    version_list
  LIMIT
    1
));

SELECT
  assert_equals('10.14', udf_normalize_os_version('Darwin', '18.5.0')),
  assert_equals('10.14', udf_normalize_os_version('Darwin', '18.9.0')),
  assert_equals('10.13', udf_normalize_os_version('Darwin', '17.0.0')),
  assert_equals('10.15', udf_normalize_os_version('Darwin', '19.5.0')),
  assert_null(udf_normalize_os_version('Darwin', '20.5.0')),
  assert_equals('95', udf_normalize_os_version('Windows_95', '4.0')),
  assert_equals('98', udf_normalize_os_version('Windows_98', '4.10')),
  assert_equals('NT4.0', udf_normalize_os_version('Windows_NT', '4.0')),
  assert_null(udf_normalize_os_version('Windows_NT', '4')),
  assert_equals('7.0', udf_normalize_os_version('Android', '24')),
  assert_equals('1', udf_normalize_os_version('Linux', '1')),
  assert_equals('2', udf_normalize_os_version('Other', '2'))
