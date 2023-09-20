{{ is_unique(["`date`", "country"]) }}
 {{ min_row_count(1, "`date` = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)") }}
