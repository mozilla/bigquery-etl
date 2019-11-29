/*

Static lookup table for normalized os names and versions stored in the get_normalized_os_list udf

*/

SELECT
  *
FROM
  UNNEST(udf_get_normalized_os_list())
