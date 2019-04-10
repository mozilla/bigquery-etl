CREATE TEMP FUNCTION
  udf_null_if_empty_list(list ANY TYPE) AS ( IF(ARRAY_LENGTH(list.list) > 0,
      list,
      NULL) );
