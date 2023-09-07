{{ is_unique(["`date`", "country"]) }}
 {{ min_rows(1, "`date` = DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)") }}

